//! Frozen Go child-progress counts on a run-owned migrated PostgreSQL database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn read(app: &axum::Router, ws: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut request =
        Request::builder().uri(format!("/api/issues/child-progress?workspace_id={ws}"));
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn child_progress_counts_terminal_custom_statuses_without_cross_workspace_leaks() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        member,
        outsider,
        ws,
        foreign_ws,
        parent,
        foreign_parent,
    ]: [Uuid; 7] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            r#"INSERT INTO "user"(id,name,email) VALUES ($1,'progress-test',$2)"#,
            &[&user, &format!("{user}@progress.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'progress',$2)",
            &[&workspace, &format!("progress-{workspace}")],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner'),($4,$3,'member')", &[&ws,&owner,&member,&foreign_ws]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, b"synthetic-progress".to_vec()),
    );
    let jwt = |user| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-progress")).unwrap()
    };
    assert_eq!(
        read(&app, ws, Some(&jwt(owner))).await,
        (StatusCode::OK, json!({"progress":[]}))
    );
    assert_eq!(read(&app, ws, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, ws, Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    let pat = format!("mul_progress_{parent}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'progress',$2,'mul_progress')", &[&member,&hash]).await.unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position,archived_at) VALUES ($1,'shipped','Shipped','done','#00aa00',40,now()),($1,'abandoned','Abandoned','closed','#aa0000',41,now())", &[&ws]).await.unwrap();
    for (id, workspace) in [(parent, ws), (foreign_parent, foreign_ws)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'parent','todo','member',$3,1)", &[&id,&workspace,&owner]).await.unwrap();
    }
    for (number, status) in [
        (2, "todo"),
        (3, "done"),
        (4, "cancelled"),
        (5, "shipped"),
        (6, "abandoned"),
        (7, "in_progress"),
    ] {
        db.execute("INSERT INTO issue(workspace_id,parent_issue_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'child',$3,'member',$4,$5)", &[&ws,&parent,&status,&owner,&number]).await.unwrap();
    }
    db.execute("INSERT INTO issue(workspace_id,parent_issue_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'foreign','done','member',$3,2)", &[&foreign_ws,&foreign_parent,&owner]).await.unwrap();
    let expected = (
        StatusCode::OK,
        json!({"progress":[{"parent_issue_id":parent,"total":6,"done":4}]}),
    );
    assert_eq!(read(&app, ws, Some(&jwt(owner))).await, expected);
    assert_eq!(read(&app, ws, Some(&jwt(member))).await, expected);
    assert_eq!(read(&app, ws, Some(&pat)).await, expected);
    assert_eq!(
        read(&app, foreign_ws, Some(&jwt(member))).await,
        (
            StatusCode::OK,
            json!({"progress":[{"parent_issue_id":foreign_parent,"total":1,"done":1}]})
        )
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.0, StatusCode::UNAUTHORIZED);
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, Some(&jwt(member))).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "ALTER TABLE issue_status RENAME TO issue_status_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to resolve status categories"})
        )
    );
    db.execute(
        "ALTER TABLE issue_status_unavailable RENAME TO issue_status",
        &[],
    )
    .await
    .unwrap();
    db.execute("ALTER TABLE issue RENAME TO issue_unavailable", &[])
        .await
        .unwrap();
    assert_eq!(
        read(&app, ws, Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get child issue progress"})
        )
    );
    db.execute("ALTER TABLE issue_unavailable RENAME TO issue", &[])
        .await
        .unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM issue WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, i64>(0),
        7
    );
}
