//! Synthetic autopilot history reads; no provider, scheduler or installed agent.
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

const SECRET: &[u8] = b"synthetic-autopilot-history-secret";

async fn get(
    app: &axum::Router,
    path: &str,
    token: Option<&str>,
    cookie: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = if cookie {
            request.header("cookie", format!("multica_auth={token}"))
        } else {
            request.header("authorization", format!("Bearer {token}"))
        };
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn snapshot(db: &tokio_postgres::Client) -> Value {
    db.query_one("SELECT jsonb_build_object('autopilots',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM autopilot t),'runs',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM autopilot_run t),'deliveries',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM webhook_delivery t),'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM autopilot_trigger t))", &[]).await.unwrap().get(0)
}

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi63 PostgreSQL with both migration streams"]
async fn history_preserves_auth_scope_shapes_pagination_and_no_writes() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("history-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let mut workspaces = Vec::new();
    for name in ["a", "b"] {
        workspaces.push(
            db.query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("history-{name}-{suffix}")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let (ws, foreign_ws) = (workspaces[0], workspaces[1]);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&foreign_ws]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'History runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'History actor','cloud',$2,$3) RETURNING id", &[&ws,&users[0],&runtime]).await.unwrap().get(0);
    let mut autopilots = Vec::new();
    for workspace in [ws, ws, foreign_ws] {
        autopilots.push(db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,created_by_type,created_by_id,status) VALUES ($1,'Archived history',$2,'member',$3,'archived') RETURNING id", &[&workspace,&agent,&users[0]]).await.unwrap().get::<_,Uuid>(0));
    }
    let (ap, empty, foreign_ap) = (autopilots[0], autopilots[1], autopilots[2]);
    let trigger: Uuid = db.query_one("INSERT INTO autopilot_trigger(autopilot_id,kind,webhook_token,signing_secret) VALUES ($1,'webhook','never-echo-token','never-echo-secret') RETURNING id", &[&ap]).await.unwrap().get(0);
    let mut runs = Vec::new();
    for index in 0..3_i32 {
        runs.push(db.query_one("INSERT INTO autopilot_run(autopilot_id,source,status,trigger_payload,result,reason_code,created_at) VALUES ($1,'webhook','completed',$2,$3,$4,'2026-01-01T00:00:00Z'::timestamptz + ($5::int * interval '1 second')) RETURNING id", &[&ap,&json!({"payload":index}),&json!({"result":index}),&if index == 2 {Some("")} else {None},&index]).await.unwrap().get::<_,Uuid>(0));
    }
    let mut deliveries = Vec::new();
    for index in 0..3_i32 {
        let body: Vec<u8> = if index == 2 {
            vec![b'a', 0xff, 0xfe, b'b']
        } else {
            Vec::new()
        };
        deliveries.push(db.query_one("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,selected_headers,raw_body,response_body,created_at) VALUES ($1,$2,$3,'generic',$4,$5,$6,'2026-01-01T00:00:00Z'::timestamptz + ($7::int * interval '1 second')) RETURNING id", &[&ws,&ap,&trigger,&json!({"event":"synthetic"}),&body,&if index == 2 {Some("")} else {None},&index]).await.unwrap().get::<_,Uuid>(0));
    }
    // Intentionally damaged relation: frozen Go list misses delivery workspace.
    let foreign_delivery: Uuid = db.query_one("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,raw_body) VALUES ($1,$2,$3,'generic',$4) RETURNING id", &[&foreign_ws,&ap,&trigger,&b"foreign-private".to_vec()]).await.unwrap().get(0);
    let pat = format!("mul_history_{suffix}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'history',$2,'mul_history')", &[&users[0],&pat_hash]).await.unwrap();
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_history_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&users[0]]).await.unwrap();
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
    };
    let owner = jwt(users[0]);
    let member = jwt(users[1]);
    let outsider = jwt(users[2]);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "history-test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let before = snapshot(&db).await;
    let path = |tail: &str| format!("/api/autopilots/{ap}/{tail}?workspace_id={ws}");
    let list = get(&app, &path("runs"), Some(&owner), false).await;
    assert_eq!(list.0, StatusCode::OK, "{list:?}");
    assert_eq!(list.1["total"], 3);
    assert_eq!(list.1["runs"][0]["id"], runs[2].to_string());
    assert_eq!(list.1["runs"][0]["reason_code"], "");
    assert!(list.1["runs"][1].get("reason_code").is_none());
    assert!(list.1["runs"][0]["trigger_payload"].is_null());
    for key in [
        "trigger_id",
        "issue_id",
        "task_id",
        "completed_at",
        "failure_reason",
    ] {
        assert!(list.1["runs"][0][key].is_null(), "{key}");
    }
    assert_eq!(list.1["runs"][0]["created_at"], "2026-01-01T00:00:02Z");
    for token in [&member, &pat, &task_token] {
        assert_eq!(get(&app, &path("runs"), Some(token), false).await, list);
    }
    assert_eq!(get(&app, &path("runs"), Some(&owner), true).await, list);
    let detail = get(
        &app,
        &path(&format!("runs/{}", runs[2])),
        Some(&owner),
        false,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK);
    assert_eq!(detail.1["trigger_payload"], json!({"payload":2}));
    let delivery_list = get(&app, &path("deliveries"), Some(&member), false).await;
    assert_eq!(delivery_list.0, StatusCode::OK, "{delivery_list:?}");
    assert_eq!(delivery_list.1["total"], 3);
    for delivery in delivery_list.1["deliveries"].as_array().unwrap() {
        for field in [
            "selected_headers",
            "raw_body",
            "response_body",
            "signing_secret",
            "webhook_token",
        ] {
            assert!(delivery.get(field).is_none(), "{field}");
        }
        for field in [
            "dedupe_key",
            "error",
            "reason_code",
            "autopilot_run_id",
            "replay_idempotency_key",
        ] {
            assert!(delivery[field].is_null(), "{field}");
        }
        assert_eq!(delivery["workspace_id"], ws.to_string());
    }
    let full = get(
        &app,
        &path(&format!("deliveries/{}", deliveries[2])),
        Some(&owner),
        false,
    )
    .await;
    assert_eq!(full.0, StatusCode::OK, "{full:?}");
    assert_eq!(full.1["raw_body"], "a\u{fffd}\u{fffd}b");
    assert_eq!(full.1["response_body"], "");
    assert_eq!(full.1["selected_headers"], json!({"event":"synthetic"}));
    let blank = get(
        &app,
        &path(&format!("deliveries/{}", deliveries[0])),
        Some(&owner),
        false,
    )
    .await;
    assert!(blank.1.get("raw_body").is_none());
    assert!(blank.1.get("response_body").is_none());
    for kind in ["runs", "deliveries"] {
        let key = kind;
        for (query, count, first) in [
            ("limit=1&offset=1", 1, 1),
            ("limit=1&limit=2&offset=0&offset=2", 1, 2),
            ("limit=0&offset=-1", 3, 2),
            ("limit=200", 3, 2),
            ("limit=4294967296", 0, 0),
            ("offset=99", 0, 0),
        ] {
            let result = get(
                &app,
                &format!("{}&{query}", path(kind)),
                Some(&owner),
                false,
            )
            .await;
            assert_eq!(result.0, StatusCode::OK, "{kind} {query} {result:?}");
            assert_eq!(result.1["total"], count);
            if count > 0 {
                assert_eq!(
                    result.1[key][0]["id"],
                    if kind == "runs" {
                        runs[first]
                    } else {
                        deliveries[first]
                    }
                    .to_string()
                );
            }
        }
        for query in ["limit=2147483648", "offset=4294967295"] {
            assert_eq!(
                get(
                    &app,
                    &format!("{}&{query}", path(kind)),
                    Some(&owner),
                    false
                )
                .await,
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"error":format!("failed to list {kind}")})
                )
            );
        }
        let mut expected = json!({"total":0});
        expected[key] = json!([]);
        assert_eq!(
            get(
                &app,
                &format!("/api/autopilots/{empty}/{kind}?workspace_id={ws}"),
                Some(&owner),
                false
            )
            .await,
            (StatusCode::OK, expected)
        );
        assert_eq!(
            get(&app, &path(kind), None, false).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&app, &path(kind), Some("invalid"), false).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&app, &path(kind), Some("mdt_synthetic_absent"), false)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get(&app, &path(kind), Some(&outsider), false).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(
                &app,
                &format!("/api/autopilots/{foreign_ap}/{kind}?workspace_id={ws}"),
                Some(&owner),
                false
            )
            .await,
            (
                StatusCode::NOT_FOUND,
                json!({"error":"autopilot not found"})
            )
        );
        assert_eq!(
            get(
                &app,
                &format!("/api/autopilots/{foreign_ap}/{kind}?workspace_id={foreign_ws}"),
                Some(&task_token),
                false
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            get(
                &app,
                &format!("/api/autopilots/invalid/{kind}?workspace_id={ws}"),
                Some(&owner),
                false
            )
            .await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid autopilot id"})
            )
        );
    }
    for (kind, id, label) in [
        ("runs", runs[0], "run"),
        ("deliveries", deliveries[0], "delivery"),
    ] {
        assert_eq!(
            get(&app, &path(&format!("{kind}/invalid")), Some(&owner), false).await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":format!("invalid {label} id")})
            )
        );
        for (parent, child) in [(empty, id), (ap, Uuid::new_v4())] {
            assert_eq!(
                get(
                    &app,
                    &format!("/api/autopilots/{parent}/{kind}/{child}?workspace_id={ws}"),
                    Some(&owner),
                    false
                )
                .await,
                (
                    StatusCode::NOT_FOUND,
                    json!({"error":format!("{label} not found")})
                )
            );
        }
    }
    assert_eq!(
        get(
            &app,
            &path(&format!("deliveries/{foreign_delivery}")),
            Some(&owner),
            false
        )
        .await,
        (StatusCode::NOT_FOUND, json!({"error":"delivery not found"}))
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path("runs"), Some(&pat), false).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        snapshot(&db).await,
        before,
        "history GET mutated domain rows"
    );
    // Fault injection is confined to this disposable database; restore before assertions.
    for (table, kind, label) in [
        ("autopilot_run", "runs", "run"),
        ("webhook_delivery", "deliveries", "delivery"),
    ] {
        db.batch_execute(&format!(
            "ALTER TABLE {table} RENAME TO history_unavailable"
        ))
        .await
        .unwrap();
        let failed_list = get(&app, &path(kind), Some(&owner), false).await;
        let failed_detail = get(
            &app,
            &path(&format!("{kind}/{}", Uuid::new_v4())),
            Some(&owner),
            false,
        )
        .await;
        db.batch_execute(&format!(
            "ALTER TABLE history_unavailable RENAME TO {table}"
        ))
        .await
        .unwrap();
        assert_eq!(
            failed_list,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":format!("failed to list {kind}")})
            )
        );
        let expected = if kind == "runs" {
            (StatusCode::NOT_FOUND, json!({"error":"run not found"}))
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":format!("failed to load {label}")}),
            )
        };
        assert_eq!(failed_detail, expected);
    }
}
