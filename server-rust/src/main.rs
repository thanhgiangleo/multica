use deadpool_postgres::{Manager, Pool};
use multica_server::{
    api::{ApiState, SessionConfig},
    health::{Health, MigrationCatalog},
    migrate::{self, Direction, Stream},
    router_with_api, scheduler,
};
use std::{
    collections::HashMap, error::Error, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc,
};
use tokio_postgres::{Config, NoTls, config::Host};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let bind: SocketAddr = std::env::var("RUST_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:18091".into())
        .parse()?;
    if !bind.ip().is_loopback() {
        return Err("Rust port is not ready for external traffic; bind to loopback".into());
    }
    let url = std::env::var("RUST_DATABASE_URL")
        .map_err(|_| "set RUST_DATABASE_URL to a disposable development database")?;
    let config = Config::from_str(&url).map_err(|_| "invalid RUST_DATABASE_URL")?;
    if config.get_hosts().is_empty()
        || config.get_hosts().iter().any(|host| match host {
            Host::Tcp(host) => host != "localhost" && host != "127.0.0.1" && host != "::1",
            #[cfg(unix)]
            Host::Unix(_) => true,
        })
    {
        return Err("RUST_DATABASE_URL must point to a loopback TCP database".into());
    }
    let mode = std::env::args().nth(1);
    if mode.is_some() && !matches!(mode.as_deref(), Some("migrate-up" | "migrate-down")) {
        return Err("usage: multica-server [migrate-up|migrate-down]".into());
    }
    if mode.is_some() {
        let name = config
            .get_dbname()
            .ok_or("migration database name required")?;
        if std::env::var("RUST_DEV_DATABASE_CONFIRM").ok().as_deref() != Some(name)
            || !name.starts_with("multica_")
        {
            return Err(
                "set RUST_DEV_DATABASE_CONFIRM to the exact disposable multica_* database name"
                    .into(),
            );
        }
    }
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(8)
        .build()?;
    if let Some(mode) = mode {
        let root = PathBuf::from(std::env::var("RUST_MIGRATIONS_ROOT")?);
        let hooks: serde_json::Value =
            serde_json::from_str(include_str!("../baseline/concurrent-index-hooks.json"))?;
        let up: HashMap<String, String> = serde_json::from_value(hooks["upstream_up"].clone())?;
        let down: HashMap<String, String> = serde_json::from_value(hooks["upstream_down"].clone())?;
        let zbs_up: HashMap<String, String> = serde_json::from_value(hooks["zbs_up"].clone())?;
        let no_hooks = HashMap::new();
        let direction = if mode == "migrate-up" {
            Direction::Up
        } else {
            Direction::Down
        };
        let upstream = Stream {
            directory: &root.join("migrations"),
            table: "schema_migrations",
            lock_key: 0,
            hooks: if direction == Direction::Up {
                &up
            } else {
                &down
            },
            production_gates: true,
        };
        let zbs_stream = Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 0,
            hooks: if direction == Direction::Up {
                &zbs_up
            } else {
                &no_hooks
            },
            production_gates: true,
        };
        let (first, second) = migrate::run_both(&pool, &upstream, &zbs_stream, direction).await?;
        println!("upstream={first:?} zbs={second:?}");
        return Ok(());
    }
    let catalog = MigrationCatalog::embedded()?;
    let pool = Arc::new(pool);
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        catalog,
        option_env!("RUST_SOURCE_REVISION")
            .unwrap_or("unknown")
            .into(),
    ));
    let secret =
        std::env::var("JWT_SECRET").map_err(|_| "JWT_SECRET required for authenticated APIs")?;
    if secret.is_empty()
        || matches!(
            secret.as_str(),
            "multica-dev-secret-change-in-production" | "change-me-in-production"
        )
    {
        return Err("JWT_SECRET must not be a known insecure default".into());
    }
    let session = SessionConfig::from_env()?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let mut scheduler = scheduler::Manager::new(pool.clone());
    scheduler.register(scheduler::prune_search_index_changes())?;
    scheduler.register(scheduler::issue_wakeup_job())?;
    let (stop_scheduler, scheduler_shutdown) = tokio::sync::watch::channel(false);
    let sweep_task = tokio::spawn(
        multica_server::api::p0_daemon_outcomes::run_runtime_sweeper(
            pool.clone(),
            multica_server::api::p0_daemon_outcomes::runtime_reconnect_grace(),
            scheduler_shutdown.clone(),
        ),
    );
    let scheduler_task = tokio::spawn(scheduler.run(scheduler_shutdown.clone()));
    println!(
        "rust backend read slice listening on {} (not full backend)",
        listener.local_addr()?
    );
    let state = ApiState::new(pool, secret.into_bytes())
        .with_realtime_token(std::env::var("REALTIME_METRICS_TOKEN").ok())
        .with_local_inbox_events()
        .with_local_pin_events()
        .with_local_p0_agent_events()
        .with_local_p0_task_events()
        .with_local_daemon_wake()
        .with_invitation_admission_from_env()
        .with_session_config(session)
        .with_workspace_integrations(
            multica_server::api::workspace_integrations::IntegrationConfig::from_env(),
        )
        .with_cloud_billing(
            multica_server::api::cloud_billing::CloudRuntimeConfig::from_env(),
            multica_server::api::cloud_billing::CloudBillingFlagProvider::from_env(),
        )
        .with_cloud_url(&std::env::var("MULTICA_CLOUD_URL").unwrap_or_default())?;
    // Go registers its subscriber listeners before the server starts serving, so
    // no committed comment can be published before something is listening.
    let subscriber_listener =
        multica_server::api::comment_subscriber_listener::spawn(&state, scheduler_shutdown.clone());
    let served = axum::serve(
        listener,
        router_with_api(health, state)
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await;
    let _ = stop_scheduler.send(true);
    if let Err(error) = scheduler_task.await {
        eprintln!("scheduler join: {error}");
    }
    if let Err(error) = sweep_task.await {
        eprintln!("runtime sweeper join: {error}");
    }
    if let Err(error) = subscriber_listener.await {
        eprintln!("comment subscriber listener join: {error}");
    }
    served?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
