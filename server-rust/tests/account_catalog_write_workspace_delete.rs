//! Synthetic HTTP/DB contract for the provisional self-host workspace delete.
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
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "synthetic-workspace-delete-secret";
fn jwt(user: Uuid, expired: bool) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+if expired {-300} else {3600}}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    cookie: bool,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(auth) = authorization {
        builder = if cookie {
            builder.header("cookie", format!("multica_auth={auth}"))
        } else {
            builder.header("authorization", format!("Bearer {auth}"))
        };
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    if status == StatusCode::NO_CONTENT {
        assert!(bytes.is_empty());
    }
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    ws: Uuid,
    neighbour: Uuid,
    owner: Uuid,
    admin: Uuid,
    outsider: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(10)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let mut users = Vec::new();
        for role in ["Owner", "Admin", "Outsider"] {
            users.push(
                db.query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[&role, &format!("delete-{}@invalid.example", Uuid::new_v4())],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [owner, admin, outsider] = <[Uuid; 3]>::try_from(users).unwrap();
        let mut workspaces = Vec::new();
        for name in ["Delete", "Neighbour"] {
            workspaces.push(
                db.query_one(
                    "INSERT INTO workspace(name,slug) VALUES($1,$2) RETURNING id",
                    &[&name, &format!("delete-{}", Uuid::new_v4())],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [ws, neighbour] = <[Uuid; 2]>::try_from(workspaces).unwrap();
        db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin'),($4,$2,'owner')",
            &[&ws,&owner,&admin,&neighbour]).await.unwrap();
        let app = app(pool.clone(), false);
        Self {
            pool,
            app,
            ws,
            neighbour,
            owner,
            admin,
            outsider,
        }
    }
    fn path(&self) -> String {
        format!("/api/workspaces/{}", self.ws)
    }
    async fn exists(&self) -> bool {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM workspace WHERE id=$1)",
                &[&self.ws],
            )
            .await
            .unwrap()
            .get(0)
    }
}
fn app(pool: Arc<Pool>, cloud: bool) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec());
    if cloud {
        state = state.with_cloud_url("http://127.0.0.1:1").unwrap();
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    )
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn delete_authentication_actor_roles_and_scope() {
    let f = Fixture::new().await;
    let path = f.path();
    for (credential, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(jwt(f.owner, true)), StatusCode::UNAUTHORIZED),
        (Some(jwt(f.outsider, false)), StatusCode::NOT_FOUND),
        (Some(jwt(f.admin, false)), StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            request(&f.app, "DELETE", &path, credential.as_deref(), false)
                .await
                .0,
            expected
        );
    }
    assert_eq!(
        request(&f.app, "DELETE", &path, Some(&jwt(f.owner, false)), true)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            "/api/workspaces/invalid",
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let db = f.pool.get().await.unwrap();
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked) VALUES($1,'revoked',$2,'mul_test',true)", &[&f.owner,&hash]).await.unwrap();
    assert_eq!(
        request(&f.app, "DELETE", &path, Some(&pat), false).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE personal_access_token SET revoked=false,expires_at=now()-interval '1 hour' WHERE token_hash=$1", &[&hash]).await.unwrap();
    assert_eq!(
        request(&f.app, "DELETE", &path, Some(&pat), false).await.0,
        StatusCode::UNAUTHORIZED
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Task actor','cloud',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Actor runtime','cloud','synthetic','online') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Actor issue','member',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let task: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,issue_id,runtime_id,status) VALUES($1,$2,$3,'running') RETURNING id",
            &[&agent,&issue,&runtime],
        )
        .await
        .unwrap()
        .get(0);
    let task_token = format!("mat_delete_{}", Uuid::new_v4().simple());
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    assert_eq!(
        request(&f.app, "DELETE", &path, Some(&task_token), false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}", f.neighbour),
            Some(&task_token),
            false
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&f.owner],
    )
    .await
    .unwrap();
    assert_eq!(
        request(&f.app, "DELETE", &path, Some(&jwt(f.owner, false)), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert!(f.exists().await);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn delete_graph_and_cross_workspace_are_transactional() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    for ws in [f.ws, f.neighbour] {
        db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,'todo','Todo','unstarted','#999999')", &[&ws]).await.unwrap();
        db.execute("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,is_default) VALUES($1,'Default','todo',true)", &[&ws]).await.unwrap();
        let project: Uuid = db
            .query_one(
                "INSERT INTO project(workspace_id,title) VALUES($1,'Delete project') RETURNING id",
                &[&ws],
            )
            .await
            .unwrap()
            .get(0);
        db.execute("INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref) VALUES($1,$2,'github_repo',$3)", &[&project,&ws,&json!({"owner":"synthetic","repo":"test"})]).await.unwrap();
        let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Delete issue','member',$2) RETURNING id", &[&ws,&f.owner]).await.unwrap().get(0);
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&issue,&f.owner]).await.unwrap();
        db.execute("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role,expires_at) VALUES($1,$2,'test@invalid.example','member',now()+interval '7 days')", &[&ws,&f.owner]).await.unwrap();
        let package:Uuid=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name) VALUES($1,'com.synthetic.test','Synthetic') RETURNING id", &[&ws]).await.unwrap().get(0);
        let version:Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1','{}',repeat('0',64),0) RETURNING id", &[&package,&ws]).await.unwrap().get(0);
        db.execute("INSERT INTO plugin_package_file(version_id,path,content,size_bytes,sha256) VALUES($1,'file',decode('','hex'),0,repeat('0',64))", &[&version]).await.unwrap();
        let plugin:Uuid=db.query_one("INSERT INTO plugin_installation(workspace_id,plugin_key,package_version_id,version,manifest) VALUES($1,'com.synthetic.test',$2,'1','{}') RETURNING id", &[&ws,&version]).await.unwrap().get(0);
        db.execute("INSERT INTO plugin_storage(installation_id,scope_type,scope_id,key,value) VALUES($1,'workspace',$2,'key','value')", &[&plugin,&ws]).await.unwrap();
    }
    // Fail late: even already swept leaf rows must roll back with the workspace.
    db.batch_execute("CREATE FUNCTION pg_temp.reject_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic workspace fault'; END $$; CREATE TRIGGER account_catalog_delete_fault BEFORE DELETE ON workspace FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_delete()").await.unwrap();
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.exists().await);
    for table in [
        "issue_status",
        "issue_workflow",
        "issue",
        "plugin_installation",
        "workspace_invitation",
    ] {
        let n: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE workspace_id=$1"),
                &[&f.ws],
            )
            .await
            .unwrap()
            .get(0);
        assert!(n > 0, "rollback lost {table}");
    }
    db.batch_execute("DROP TRIGGER account_catalog_delete_fault ON workspace")
        .await
        .unwrap();
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &format!("{}/", f.path()),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(!f.exists().await);
    for table in [
        "issue_status",
        "issue_workflow",
        "project",
        "project_resource",
        "issue",
        "plugin_installation",
        "workspace_invitation",
    ] {
        let n: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE workspace_id=$1"),
                &[&f.ws],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(n, 0, "orphan {table}");
        let foreign: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE workspace_id=$1"),
                &[&f.neighbour],
            )
            .await
            .unwrap()
            .get(0);
        assert!(foreign > 0, "deleted neighbour {table}");
    }
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn delete_live_work_and_external_dependencies_fail_closed() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Task','member',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Task owner','cloud',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let foreign_runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Foreign runtime','cloud','synthetic','online') RETURNING id", &[&f.neighbour]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,runtime_id,status) VALUES($1,$2,$3,'queued') RETURNING id", &[&agent,&issue,&foreign_runtime]).await.unwrap().get(0);
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.exists().await);
    db.execute(
        "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic','cloud','synthetic','offline') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute("DELETE FROM agent_runtime WHERE id=$1", &[&runtime])
        .await
        .unwrap();
    db.execute("INSERT INTO issue_source_context_object_intent(storage_key,workspace_id,source_context_id,attachment_id,object_url) VALUES($1,$2,$3,$4,'https://invalid.example/object')", &[&Uuid::new_v4().to_string(),&f.ws,&Uuid::new_v4(),&Uuid::new_v4()]).await.unwrap();
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.exists().await);
    let cloud = app(f.pool.clone(), true);
    assert_eq!(
        request(
            &cloud,
            "DELETE",
            &f.path(),
            Some(&jwt(f.owner, false)),
            false
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        db.query_one(
            "SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE id=$1)",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn delete_owner_demotion_while_waiting_and_concurrent_retry() {
    let f = Fixture::new().await;
    let mut blocker = f.pool.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    let blocker_pid: i32 = tx
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    tx.query_one("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&f.ws])
        .await
        .unwrap();
    let app = f.app.clone();
    let path = f.path();
    let credential = jwt(f.owner, false);
    let pending =
        tokio::spawn(async move { request(&app, "DELETE", &path, Some(&credential), false).await });
    let observer = f.pool.get().await.unwrap();
    // Observe the actual lock wait, not a timing-dependent sleep before demotion.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = observer.query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                &[&blocker_pid],
            ).await.unwrap().get(0);
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tx.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&f.ws, &f.owner],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let (status, body) = pending.await.unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(f.exists().await);
    observer
        .execute(
            "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2",
            &[&f.ws, &f.owner],
        )
        .await
        .unwrap();
    let credential = jwt(f.owner, false);
    let path = f.path();
    let (a, b) = tokio::join!(
        request(&f.app, "DELETE", &path, Some(&credential), false),
        request(&f.app, "DELETE", &path, Some(&credential), false),
    );
    let mut codes = [a.0.as_u16(), b.0.as_u16()];
    codes.sort();
    assert_eq!(codes, [204, 404], "responses: {a:?}, {b:?}");
    assert!(!f.exists().await);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn delete_task_pages_and_owner_lock_serialization() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Task owner','member',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Other owner','member',$2) RETURNING id", &[&f.neighbour,&f.owner]).await.unwrap().get(0);
    // No cascade may do the application's cleanup. This entire database is
    // disposable and all fixtures assert its exact name and loopback endpoint.
    db.batch_execute("DO $$ DECLARE r record; BEGIN FOR r IN SELECT conrelid::regclass AS tbl,conname FROM pg_constraint WHERE contype='f' AND connamespace='public'::regnamespace LOOP EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',r.tbl,r.conname); END LOOP; END $$").await.unwrap();
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Foreign agent','cloud',$2) RETURNING id", &[&f.neighbour,&f.owner]).await.unwrap().get(0);
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES($1,$2,'Zero UUID owner','cloud',$3)", &[&Uuid::nil(),&f.ws,&f.owner]).await.unwrap();
    db.execute("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) SELECT $1,'Paged owner '||n,'cloud',$2 FROM generate_series(1,501) AS n", &[&f.ws,&f.owner]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(agent_id,issue_id,status,completed_at) SELECT $1,$2,'completed',now() FROM generate_series(1,1003)", &[&agent,&issue]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(agent_id,issue_id,status,completed_at) VALUES($1,$2,'completed',now()),($3,$2,'completed',now())", &[&agent,&other,&Uuid::nil()]).await.unwrap();
    let first: Uuid = db
        .query_one(
            "SELECT id FROM agent_task_queue WHERE issue_id=$1 LIMIT 1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks) VALUES($1,'synthetic','model',0,0,0,0,0)", &[&first]).await.unwrap();
    // Held workspace lock must bound the request and preserve all rows on retry.
    let mut blocker = f.pool.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.query_one("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&f.ws])
        .await
        .unwrap();
    let app = f.app.clone();
    let path = f.path();
    let credential = jwt(f.owner, false);
    let future = tokio::spawn(async move {
        request(&app, "DELETE", &path, Some(&credential), false)
            .await
            .0
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    tx.rollback().await.unwrap();
    assert_eq!(future.await.unwrap(), StatusCode::NO_CONTENT);
    assert!(!f.exists().await);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1",
            &[&other]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_usage WHERE task_id=$1",
            &[&first]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}
