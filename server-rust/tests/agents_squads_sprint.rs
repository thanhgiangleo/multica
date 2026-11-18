//! Synthetic DB-only squad mutation tests. No installed agents or external providers.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        agents_squads_sprint::{SquadEvent, SquadEventSink},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "agents_squads_sprint_bindings/cases.rs"]
mod bindings;

#[path = "agents_squads_sprint_settings/cases.rs"]
mod settings;

#[path = "agents_squads_sprint_agents/cases.rs"]
mod agents;

#[path = "agents_squads_sprint_read/cases.rs"]
mod read;

const SECRET: &[u8] = b"synthetic-squad-config-only";
#[derive(Default)]
struct Events(Mutex<Vec<SquadEvent>>);
impl SquadEventSink for Events {
    fn publish(&self, event: SquadEvent) {
        self.0.lock().unwrap().push(event);
    }
}
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user.to_string(),"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
async fn call(
    app: &axum::Router,
    method: Method,
    path: &str,
    auth: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {auth}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let reply = app.clone().oneshot(request).await.unwrap();
    let status = reply.status();
    let bytes = reply.into_body().collect().await.unwrap().to_bytes();
    let data = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, data)
}

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn squad_mutations_enforce_permissions_atomicity_and_assignee_transfers() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi70"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for label in ["owner", "creator", "other", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("sq-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Sq work',$1) RETURNING id",
            &[&format!("sqa-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other work',$1) RETURNING id",
            &[&format!("sqb-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (workspace, user, role) in [
        (ws, users[0], "owner"),
        (ws, users[1], "member"),
        (ws, users[2], "member"),
        (foreign, users[0], "owner"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let leader: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Creator private','cloud',$2,$3) RETURNING id", &[&ws,&users[1],&runtime]).await.unwrap().get(0);
    let private: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Other private','cloud',$2,$3) RETURNING id", &[&ws,&users[2],&runtime]).await.unwrap().get(0);
    let unbound: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Unbound','cloud',$2) RETURNING id", &[&ws,&users[1]]).await.unwrap().get(0);
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Foreign','cloud',$2) RETURNING id", &[&foreign,&users[0]]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    let no_bus = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let create = format!("/api/squads/?workspace_id={ws}");
    let body = json!({"name":"Real team","leader_id":leader});
    assert_eq!(
        call(&no_bus, Method::POST, &create, &jwt(users[1]), body.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&app, Method::POST, &create, &jwt(users[3]), body.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/squads/?workspace_id={foreign}"),
            &jwt(users[1]),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &create,
            &jwt(users[1]),
            json!({"name":"No","leader_id":private})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &create,
            &jwt(users[1]),
            json!({"name":"No","leader_id":foreign_agent})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &create,
            &jwt(users[1]),
            json!({"name":"No","leader_id":leader,"avatar_url":"/uploads/workspaces/foreign.png"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, squad) = call(&app, Method::POST, &create, &jwt(users[1]), body).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = Uuid::parse_str(squad["id"].as_str().unwrap()).unwrap();
    assert_eq!(squad["member_count"], 1);
    assert_eq!(squad["member_preview"][0]["member_id"], leader.to_string());
    let path = format!("/api/squads/{id}/?workspace_id={ws}");
    let members = format!("/api/squads/{id}/members?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"name":"Hijack"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Method::DELETE, &path, &jwt(users[2]), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &members,
            &jwt(users[1]),
            json!({"member_type":"agent","member_id":private,"role":"worker"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let private_body = json!({"member_type":"agent","member_id":private,"role":"worker"});
    assert_eq!(
        call(
            &app,
            Method::POST,
            &members,
            &jwt(users[0]),
            private_body.clone()
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &jwt(users[1]),
            private_body.clone()
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&private],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&private,&users[1]]).await.unwrap();
    assert_eq!(
        call(
            &app,
            Method::POST,
            &members,
            &jwt(users[1]),
            private_body.clone()
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(&app, Method::DELETE, &members, &jwt(users[1]), private_body)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &members,
            &jwt(users[1]),
            json!({"member_type":"member","member_id":users[3]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &members,
            &jwt(users[1]),
            json!({"member_type":"agent","member_id":foreign_agent})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, member) = call(
        &app,
        Method::POST,
        &members,
        &jwt(users[1]),
        json!({"member_type":"member","member_id":users[2],"role":"worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(member["role"], "worker");
    let duplicate = json!({"member_type":"member","member_id":users[2]});
    assert_eq!(
        call(&app, Method::POST, &members, &jwt(users[1]), duplicate)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let role_path = format!("/api/squads/{id}/members/role?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            Method::PATCH,
            &role_path,
            &jwt(users[1]),
            json!({"member_type":"member","member_id":users[2],"role":"reviewer"})
        )
        .await
        .1["role"],
        "reviewer"
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &jwt(users[1]),
            json!({"member_type":"agent","member_id":leader})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::PATCH,
            &role_path,
            &jwt(users[1]),
            json!({"member_type":"agent","member_id":leader,"role":"worker"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &jwt(users[1]),
            json!({"member_type":"member","member_id":users[2]})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &jwt(users[1]),
            json!({"member_type":"member","member_id":users[2]})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // Duplicate racing additions serialize under the squad lock.
    let token = jwt(users[1]);
    let race = json!({"member_type":"member","member_id":users[0]});
    let (a, b) = tokio::join!(
        call(&app, Method::POST, &members, &token, race.clone()),
        call(&app, Method::POST, &members, &token, race)
    );
    assert!(matches!(
        (a.0, b.0),
        (StatusCode::CREATED, StatusCode::CONFLICT) | (StatusCode::CONFLICT, StatusCode::CREATED)
    ));
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &token,
            json!({"member_type":"member","member_id":users[0]})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let active: Uuid = db.query_one("INSERT INTO autopilot(workspace_id,title,created_by_type,created_by_id,assignee_type,assignee_id,status) VALUES ($1,'Active synthetic','member',$2,'squad',$3,'active') RETURNING id", &[&ws,&users[1],&id]).await.unwrap().get(0);
    let (status, updated) = call(
        &app,
        Method::PUT,
        &path,
        &jwt(users[1]),
        json!({"name":"Rotated","leader_id":unbound}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["leader_id"], unbound.to_string());
    assert_eq!(updated["member_count"], 2);
    let paused = db
        .query_one(
            "SELECT status,pause_reason FROM autopilot WHERE id=$1",
            &[&active],
        )
        .await
        .unwrap();
    assert_eq!(paused.get::<_, String>("status"), "paused");
    assert_eq!(
        paused.get::<_, Option<String>>("pause_reason").as_deref(),
        Some("agent_runtime_required")
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &members,
            &jwt(users[1]),
            json!({"member_type":"agent","member_id":unbound})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,assignee_type,assignee_id) VALUES ($1,'Transfer','member',$2,'squad',$3) RETURNING id", &[&ws,&users[1],&id]).await.unwrap().get(0);
    let pilot: Uuid = db.query_one("INSERT INTO autopilot(workspace_id,title,description,created_by_type,created_by_id,assignee_type,assignee_id,status) VALUES ($1,'Synthetic pilot','none','member',$2,'squad',$3,'paused') RETURNING id", &[&ws,&users[1],&id]).await.unwrap().get(0);
    let before_failed_archive = events.0.lock().unwrap().len();
    // Inject transfer failure; archive must roll back issue transfer and squad state.
    db.batch_execute("CREATE FUNCTION agents_squads_sprint_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rollback'; END $$; CREATE TRIGGER agents_squads_sprint_fail BEFORE UPDATE ON autopilot FOR EACH ROW EXECUTE FUNCTION agents_squads_sprint_fail()").await.unwrap();
    assert_eq!(
        call(&app, Method::DELETE, &path, &jwt(users[1]), Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one("SELECT assignee_type FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "squad"
    );
    assert!(
        db.query_one("SELECT archived_at IS NULL FROM squad WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(events.0.lock().unwrap().len(), before_failed_archive);
    db.batch_execute("DROP TRIGGER agents_squads_sprint_fail ON autopilot; DROP FUNCTION agents_squads_sprint_fail()").await.unwrap();
    assert_eq!(
        call(&app, Method::DELETE, &path, &jwt(users[1]), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, Method::DELETE, &path, &jwt(users[1]), Value::Null)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let assignee: Uuid = db
        .query_one(
            "SELECT assignee_id FROM issue WHERE id=$1 AND assignee_type='agent'",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(assignee, unbound);
    let pilot_owner: Uuid = db
        .query_one(
            "SELECT assignee_id FROM autopilot WHERE id=$1 AND assignee_type='agent'",
            &[&pilot],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pilot_owner, unbound);
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[1]),
            json!({"name":"No"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let recorded = events.0.lock().unwrap();
    assert_eq!(recorded.first().unwrap().event_type, "squad:created");
    let pause_event = recorded
        .iter()
        .find(|event| event.event_type == "autopilot:updated")
        .unwrap();
    assert_eq!(pause_event.payload["autopilot"]["id"], active.to_string());
    assert_eq!(pause_event.payload["autopilot"]["status"], "paused");
    assert_eq!(
        pause_event.payload["autopilot"]["pause_reason"],
        "agent_runtime_required"
    );
    assert_eq!(pause_event.payload["autopilot"]["subscribers"], json!([]));
    assert_eq!(recorded.last().unwrap().event_type, "squad:deleted");
    assert!(
        recorded
            .iter()
            .all(|event| event.workspace_id == ws && [users[0],users[1]].contains(&event.actor_id))
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| event.event_type == "squad:deleted")
            .count(),
        1
    );
}

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn task_token_cannot_mutate_squad_even_when_bound_to_owner() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi70"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Token owner',$1) RETURNING id",
            &[&format!("token-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Token ws',$1) RETURNING id",
            &[&format!("token-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Token runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id) VALUES ($1,'Token agent','cloud',$2,$3) RETURNING id", &[&ws,&runtime,&user]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_synthetic_squad_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&task,&agent,&ws,&user]).await.unwrap();
    let events = Arc::new(Events::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    let path = format!("/api/squads/?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            Method::POST,
            &path,
            &token,
            json!({"name":"Task forged","leader_id":agent})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert!(
        db.query_opt("SELECT id FROM squad WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .is_none()
    );
    assert!(events.0.lock().unwrap().is_empty());
}
