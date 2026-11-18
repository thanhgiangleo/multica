//! HTTP/DB facets: disjunctive dimensions, property buckets and agent identity privacy.
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
use std::{collections::BTreeMap, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const SECRET: &[u8] = b"queries-sprint-table-facets";
fn token(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
fn request(kinds: &[&str]) -> Value {
    json!({"query":{"scope":{"kind":"workspace"},"filters":{},"sort":{"field":"position"}},"facets":kinds.iter().map(|k|json!({"kind":k})).collect::<Vec<_>>()})
}
async fn post_raw(
    app: &axum::Router,
    ws: Uuid,
    auth: Option<&str>,
    body: String,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/api/issues/table/facets?workspace_id={ws}"))
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        req = req.header("authorization", format!("Bearer {auth}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("non-JSON {status}: {bytes:?}")),
    )
}
async fn post(app: &axum::Router, ws: Uuid, auth: &str, body: Value) -> (StatusCode, Value) {
    post_raw(app, ws, Some(auth), body.to_string()).await
}
fn counts(body: &Value, index: usize) -> BTreeMap<String, i64> {
    body["facets"][index]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["key"].as_str().unwrap().to_owned(),
                v["count"].as_i64().unwrap(),
            )
        })
        .collect()
}
fn expected(values: &[(&str, i64)]) -> BTreeMap<String, i64> {
    values.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn table_facets_disjunction_properties_and_workspace() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Facet member',$1) RETURNING id",
            &[&format!("facet-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Facet owner',$1) RETURNING id",
            &[&format!("facet-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Facet outsider',$1) RETURNING id",
            &[&format!("facet-out-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Facet ws',$1,'FCT') RETURNING id",&[&format!("facet-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Other facet',$1,'EXT') RETURNING id",&[&format!("facet-other-{suffix}")]).await.unwrap().get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member'),($1,$3,'owner'),($4,$3,'owner')",&[&ws,&user,&owner,&other]).await.unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Facet project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let mut issues = vec![];
    for (number, status, priority, assignee, project_id) in [
        (1, "todo", "high", Some(user), Some(project)),
        (2, "done", "high", None, None),
        (3, "todo", "low", Some(owner), Some(project)),
    ] {
        let id:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,status,priority,creator_type,creator_id,assignee_type,assignee_id,project_id) VALUES ($1,$2,'Facet issue',$3,$4,'member',$5,CASE WHEN $6::uuid IS NULL THEN NULL ELSE 'member' END,$6,$7) RETURNING id",&[&ws,&number,&status,&priority,&user,&assignee,&project_id]).await.unwrap().get(0);
        issues.push(id);
    }
    let foreign:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,status,creator_type,creator_id) VALUES ($1,1,'Foreign issue','blocked','member',$2) RETURNING id",&[&other,&owner]).await.unwrap().get(0);
    let mut labels = vec![];
    for name in ["Facet red", "Facet blue"] {
        let id:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,$2,'#112233','issue') RETURNING id",&[&ws,&name]).await.unwrap().get(0);
        labels.push(id);
    }
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$4),($2,$5),($3,$4)",
        &[&issues[0], &issues[1], &issues[2], &labels[0], &labels[1]],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "table-facets".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let auth = token(user);
    let owner_auth = token(owner);
    let basic = request(&[
        "status", "priority", "assignee", "creator", "project", "label",
    ]);
    assert_eq!(
        post_raw(&app, ws, None, basic.to_string()).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, ws, &token(outsider), basic.clone()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(&app, other, &auth, basic.clone()).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, body) = post(&app, ws, &auth, basic.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 3);
    assert_eq!(counts(&body, 0), expected(&[("done", 1), ("todo", 2)]));
    assert_eq!(counts(&body, 1), expected(&[("high", 2), ("low", 1)]));
    assert_eq!(
        counts(&body, 2),
        expected(&[
            ("__none__", 1),
            (&format!("member:{user}"), 1),
            (&format!("member:{owner}"), 1)
        ])
    );
    assert_eq!(
        counts(&body, 3),
        expected(&[(&format!("member:{user}"), 3)])
    );
    assert_eq!(
        counts(&body, 4),
        expected(&[("__none__", 1), (&project.to_string(), 2)])
    );
    assert_eq!(
        counts(&body, 5),
        expected(&[(&labels[0].to_string(), 2), (&labels[1].to_string(), 1)])
    );
    for facet in body["facets"].as_array().unwrap() {
        let values = facet["values"].as_array().unwrap();
        assert!(
            values
                .windows(2)
                .all(|w| w[0]["key"].as_str() < w[1]["key"].as_str())
        );
    }
    let mut disj = basic.clone();
    disj["query"]["filters"] = json!({"statuses":["todo"],"priorities":["high"]});
    let (status, body) = post(&app, ws, &auth, disj.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 1);
    assert_eq!(counts(&body, 0), expected(&[("todo", 1), ("done", 1)]));
    assert_eq!(counts(&body, 1), expected(&[("high", 1), ("low", 1)]));
    disj["include_total"] = json!(false);
    let (status, no_total) = post(&app, ws, &auth, disj).await;
    assert_eq!(status, StatusCode::OK, "{no_total}");
    assert_eq!(no_total["total"], 0);
    assert_eq!(body["facets"], no_total["facets"]);
    for (field, value, index) in [
        ("assignees", json!([]), 2usize),
        ("project_ids", json!([Uuid::new_v4()]), 4),
        ("label_ids", json!([Uuid::new_v4()]), 5),
    ] {
        let mut filtered = basic.clone();
        filtered["query"]["filters"][field] = value;
        let (status, body) = post(&app, ws, &auth, filtered).await;
        assert_eq!(status, StatusCode::OK, "{field}: {body}");
        assert_eq!(body["total"], 0);
        // Empty assignees is an explicit false predicate even in the batched path,
        // matching frozen Go. Other disjunctive facets remove their own dimension.
        if field == "assignees" {
            assert!(counts(&body, index).is_empty());
        } else {
            assert!(!counts(&body, index).is_empty());
        }
    }
    let mut label_disj = request(&["label"]);
    label_disj["query"]["filters"]["label_ids"] = json!([labels[0]]);
    let (status, body) = post(&app, ws, &auth, label_disj).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2);
    assert_eq!(
        counts(&body, 0),
        expected(&[(&labels[0].to_string(), 2), (&labels[1].to_string(), 1)])
    );
    let mut property_ids = vec![];
    for kind in [
        "select",
        "actor",
        "checkbox",
        "text",
        "url",
        "date",
        "number",
        "multi_text",
        "multi_url",
        "multi_select",
        "multi_actor",
    ] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,$2,$2) RETURNING id",
                &[&ws, &kind],
            )
            .await
            .unwrap()
            .get(0);
        property_ids.push(id);
        let (first, second) = match kind {
            "select" | "actor" => (json!("option-a"), json!("option-b")),
            "checkbox" => (json!(true), json!(false)),
            "multi_select" | "multi_actor" => (json!(["a", "a", "b"]), json!(["b"])),
            "number" => (json!(1), json!(2)),
            "multi_text" | "multi_url" => (json!(["arbitrary first"]), json!(["arbitrary second"])),
            _ => (json!("arbitrary first"), json!("arbitrary second")),
        };
        db.execute("UPDATE issue SET properties=properties||jsonb_build_object($1::text,$2::jsonb) WHERE id=$3",&[&id.to_string(),&first,&issues[0]]).await.unwrap();
        db.execute("UPDATE issue SET properties=properties||jsonb_build_object($1::text,$2::jsonb) WHERE id=$3",&[&id.to_string(),&second,&issues[1]]).await.unwrap();
        let mut req = request(&[]);
        req["facets"] = json!([{"kind":"property","property_id":id}]);
        let (status, body) = post(&app, ws, &auth, req).await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
        assert_eq!(body["total"], 3);
        let want = match kind {
            "select" | "actor" => expected(&[("__none__", 1), ("option-a", 1), ("option-b", 1)]),
            "checkbox" => expected(&[("__none__", 1), ("true", 1), ("false", 1)]),
            "multi_select" | "multi_actor" => expected(&[("__none__", 1), ("a", 1), ("b", 2)]),
            _ => expected(&[("__none__", 1), ("__set__", 2)]),
        };
        assert_eq!(counts(&body, 0), want, "{kind}");
    }
    let mut prop_disj = request(&[]);
    prop_disj["facets"] = json!([{"kind":"property","property_id":property_ids[0]}]);
    prop_disj["query"]["filters"]["properties"] = json!({property_ids[0].to_string():["option-a"]});
    let (status, body) = post(&app, ws, &auth, prop_disj.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 1);
    assert_eq!(counts(&body, 0).len(), 3);
    let mut no_batch = prop_disj.clone();
    no_batch["include_total"] = json!(false);
    let (status, body) = post(&app, ws, &auth, no_batch).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 0);
    assert_eq!(counts(&body, 0).len(), 3);
    db.execute(
        "UPDATE issue_property SET archived_at=now() WHERE id=$1",
        &[&property_ids[0]],
    )
    .await
    .unwrap();
    prop_disj["query"]["filters"] = json!({});
    let (status, body) = post(&app, ws, &auth, prop_disj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], "unsupported_group");
    assert_eq!(body["code"], "property_archived");
    let foreign_prop:Uuid=db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Foreign secret','text') RETURNING id",&[&other]).await.unwrap().get(0);
    for id in [foreign_prop, Uuid::new_v4()] {
        let mut req = request(&[]);
        req["facets"] = json!([{"kind":"property","property_id":id}]);
        let (status, body) = post(&app, ws, &auth, req).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "property_not_found");
    }
    // Working tasks: multiple runs count, chat/autopilot take precedence, private
    // identities stay hidden from members. Foreign issue references never count.
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Facet synthetic','cloud','synthetic','online') RETURNING id",&[&ws]).await.unwrap().get(0);
    let mut agents = vec![];
    for (name, agent_owner, permission) in [
        ("own", user, "private"),
        ("private", owner, "private"),
        ("workspace", owner, "public_to"),
        ("member", owner, "public_to"),
        ("team", owner, "public_to"),
        ("archived", user, "private"),
        ("system", user, "private"),
    ] {
        let id:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode) VALUES ($1,$2,'local',$3,$4) RETURNING id",&[&ws,&name,&agent_owner,&permission]).await.unwrap().get(0);
        agents.push(id);
        db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running')",&[&id,&runtime,&issues[0]]).await.unwrap();
    }
    db.execute("UPDATE agent SET kind='system' WHERE id=$1", &[&agents[6]])
        .await
        .unwrap();
    for (id, kind, target) in [
        (agents[2], "workspace", ws),
        (agents[3], "member", user),
        (agents[4], "team", user),
    ] {
        db.execute(
            "INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,$2,$3)",
            &[&id, &kind, &target],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1",
        &[&agents[5]],
    )
    .await
    .unwrap();
    let chat:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,runtime_id) VALUES ($1,$2,$3,$4) RETURNING id",&[&ws,&agents[0],&user,&runtime]).await.unwrap().get(0);
    let autopilot:Uuid=db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,created_by_type,created_by_id) VALUES ($1,'Facet synthetic',$2,'member',$3) RETURNING id",&[&ws,&agents[0],&user]).await.unwrap().get(0);
    let run:Uuid=db.query_one("INSERT INTO autopilot_run(autopilot_id,source,status) VALUES ($1,'manual','running') RETURNING id",&[&autopilot]).await.unwrap().get(0);
    db.execute("INSERT INTO agent_task_queue(agent_id,issue_id,status,chat_session_id,autopilot_run_id,runtime_id) VALUES ($1,$2,'running',NULL,NULL,$6),($1,$3,'running',NULL,NULL,$6),($1,$2,'running',$4,NULL,$6),($1,$2,'running',NULL,$5,$6),($1,$2,'completed',NULL,NULL,$6)",&[&agents[0],&issues[1],&foreign,&chat,&run,&runtime]).await.unwrap();
    let working = request(&["working_agents"]);
    let (status, body) = post(&app, ws, &auth, working.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let want = expected(&[
        (&agents[0].to_string(), 2),
        (&agents[2].to_string(), 1),
        (&agents[3].to_string(), 1),
    ]);
    assert_eq!(counts(&body, 0), want);
    let mut toggled = working.clone();
    toggled["query"]["filters"] = json!({"working_only":true,"working_issue_ids":[issues[0]]});
    let (status, body) = post(&app, ws, &auth, toggled).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 1);
    assert_eq!(counts(&body, 0), want);
    let (status, body) = post(&app, ws, &owner_auth, working.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(counts(&body, 0).len(), 5);
    assert!(!counts(&body, 0).contains_key(&agents[5].to_string()));
    assert!(!counts(&body, 0).contains_key(&agents[6].to_string()));
    let mut scoped = working.clone();
    scoped["query"]["scope"] = json!({"kind":"my","relation":"assigned"});
    let (status, body) = post(&app, ws, &auth, scoped).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(counts(&body, 0)[&agents[0].to_string()], 1);
    let mut empty = working.clone();
    empty["query"]["search"] = json!("not matching any title");
    let (status, body) = post(&app, ws, &auth, empty).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(counts(&body, 0).is_empty());
    for bad in [
        json!({"facets":[]}),
        json!({"facets":[{"kind":"status"},{"kind":" status "}]}),
        json!({"facets":[{"kind":"unknown"}]}),
        json!({"facets":[{"kind":"property","property_id":"invalid"}]}),
        json!({"facets":[{"kind":"status","unknown":true}]}),
        json!({"facets":vec![json!({"kind":"status"});33]}),
    ] {
        let mut req = basic.clone();
        req["facets"] = bad["facets"].clone();
        let (status, body) = post(&app, ws, &auth, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    assert_eq!(
        post_raw(&app, ws, Some(&auth), format!("{} {{}}", basic))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut oversized = basic.clone();
    oversized["query"]["search"] = json!("x".repeat(1 << 20));
    assert_eq!(
        post(&app, ws, &auth, oversized).await.0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("BEGIN; LOCK TABLE issue IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        post(&app, ws, &auth, basic.clone()),
    )
    .await;
    db.batch_execute("ROLLBACK").await.unwrap();
    let (status, body) = timeout.expect("facet deadline exceeded 12 seconds");
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");
    assert_eq!(body["error"], "query_timeout");
    assert_eq!(post(&app, ws, &auth, basic).await.0, StatusCode::OK);
}
