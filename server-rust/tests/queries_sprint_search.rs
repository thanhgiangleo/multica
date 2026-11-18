//! Real PostgreSQL search-index projections, xid cursors, retention and tenant isolation.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{collections::HashSet, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-sprint-search-test";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
fn app(pool: Arc<Pool>, enabled: bool) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-sprint-search".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()).with_local_search_index(enabled),
    )
}
async fn call(
    app: &axum::Router,
    ws: Uuid,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(format!(
            "/api/search-index/{path}{}workspace_id={ws}",
            if path.contains('?') { "&" } else { "?" }
        ));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let payload = body
        .map(|v| serde_json::to_vec(&v).unwrap())
        .unwrap_or_default();
    let response = app
        .clone()
        .oneshot(
            req.header("content-type", "application/json")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    (
        status,
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn search_index_snapshot_changes_late_commit_rollback_and_pruning() {
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
            "INSERT INTO \"user\"(name,email) VALUES ('Search user',$1) RETURNING id",
            &[&format!("queries-sprint-search-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Search outsider',$1) RETURNING id",
            &[&format!(
                "queries-sprint-search-outsider-{suffix}@invalid.example"
            )],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Search A',$1,'QRY') RETURNING id",&[&format!("queries-search-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Search B',$1,'OTH') RETURNING id",&[&format!("queries-search-other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,'shipped','Đã giao','done','#123456')",&[&ws]).await.unwrap();
    let insert = "INSERT INTO issue(workspace_id,number,title,description,status,creator_type,creator_id,updated_at) VALUES ($1,$2,$3,$4,$5,'member',$6,'2026-11-02 12:00:00.1234+00') RETURNING id";
    let mut issues = vec![];
    for (n, title, status) in [
        (1i32, "Tiếng Việt", "todo"),
        (2, "Đã giao", "shipped"),
        (3, "Duplicate", "cancelled"),
    ] {
        issues.push(
            db.query_one(insert, &[&ws, &n, &title, &"Mô tả", &status, &user])
                .await
                .unwrap()
                .get::<_, Uuid>(0),
        );
    }
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=$2",
        &[&issues[0], &issues[2]],
    )
    .await
    .unwrap();
    let private: Uuid = db
        .query_one(
            insert,
            &[
                &other,
                &1i32,
                &"PRIVATE TITLE",
                &"PRIVATE DESCRIPTION",
                &"todo",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let comment:Uuid=db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,created_at) VALUES ($1,$2,'member',$3,'Bình luận 🧪','2026-11-02 12:00:00.000001+00') RETURNING id",&[&ws,&issues[0],&user]).await.unwrap().get(0);
    db.execute("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,deleted_at) VALUES ($1,$2,'member',$3,'TOMBSTONE SECRET',now())",&[&ws,&issues[0],&user]).await.unwrap();
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,description) VALUES ($1,'Dự án','Tìm kiếm') RETURNING id",&[&ws]).await.unwrap().get(0);
    let app = app(pool.clone(), true);
    let token = jwt(user);
    assert_eq!(
        call(&app, ws, "manifest", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, ws, "manifest", Some("bad"), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, ws, "manifest", Some(&jwt(outsider)), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let disabled = crate::app(pool.clone(), false);
    assert_eq!(
        call(&disabled, ws, "manifest", Some(&token), None).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"local search index is disabled"})
        )
    );
    let (status, manifest) = call(&app, ws, "manifest", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (
            manifest["issue_count"].as_i64(),
            manifest["comment_count"].as_i64(),
            manifest["project_count"].as_i64()
        ),
        (Some(3), Some(1), Some(1))
    );
    let bytes = [
        "Tiếng Việt",
        "Đã giao",
        "Duplicate",
        "Mô tả",
        "Mô tả",
        "Mô tả",
        "Bình luận 🧪",
        "Dự án",
        "Tìm kiếm",
    ]
    .iter()
    .map(|s| s.len() as i64)
    .sum::<i64>();
    assert_eq!(manifest["text_bytes"], bytes);
    let (snapshot_status, first) = call(&app, ws, "snapshot?limit=1", Some(&token), None).await;
    assert_eq!(snapshot_status, StatusCode::OK, "snapshot: {first}");
    assert_eq!(first["issues"].as_array().unwrap().len(), 1);
    assert_eq!(first["issues"][0]["id"], issues[0].to_string());
    assert_eq!(first["issues"][0]["identifier"], "QRY-1");
    assert_eq!(
        first["issues"][0]["search_updated_at"],
        "2026-11-02T12:00:00.1234Z"
    );
    assert_eq!(first["issues"][0]["metadata"], json!({}));
    assert_eq!(first["issues"][0]["properties"], json!({}));
    assert_eq!(first["issues"][0]["description"], "Mô tả");
    assert_eq!(first["comments"].as_array().unwrap().len(), 1);
    assert_eq!(
        first["comments"][0]["created_at"],
        "2026-11-02T12:00:00.000001Z"
    );
    assert_eq!(first["comments"][0]["content"], "Bình luận 🧪");
    assert_eq!(first["projects"][0]["id"], project.to_string());
    assert_eq!(first["projects"][0]["issue_count"], 0);
    assert_eq!(first["next_after_number"], 1);
    assert_eq!(first["done"], false);
    assert!(first["issues"][0].get("labels").is_none());
    assert!(first["issues"][0].get("source_context").is_none());
    let (_, page) = call(
        &app,
        ws,
        "snapshot?after_number=1&limit=2",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(page["issues"][0]["status_category"], "done");
    assert_eq!(page["issues"][0]["status_name"], "Đã giao");
    assert_eq!(
        page["issues"][1]["duplicate_of"],
        json!({"id":issues[0],"identifier":"QRY-1","title":"Tiếng Việt","status":"todo"})
    );
    assert_eq!(page["projects"], json!([]));
    assert_eq!(page["comments"], json!([]));
    assert_eq!(page["done"], false);
    assert_eq!(
        call(&app, ws, "snapshot?after_number=3", Some(&token), None)
            .await
            .1,
        json!({"issues":[],"comments":[],"projects":[],"next_after_number":3,"done":true})
    );
    for suffix in [
        "snapshot?after_number=-1",
        "snapshot?after_number=2147483648",
        "snapshot?limit=0",
        "snapshot?limit=wat",
    ] {
        assert_eq!(
            call(&app, ws, suffix, Some(&token), None).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(&app, ws, "snapshot?limit=1&limit=0", Some(&token), None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            ws,
            "snapshot?limit=9223372036854775807",
            Some(&token),
            None
        )
        .await
        .0,
        StatusCode::OK
    );
    // A mark to a foreign issue is never expanded into a cross-tenant reference.
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=$2",
        &[&private, &issues[2]],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, ws, "snapshot?after_number=2", Some(&token), None)
            .await
            .1["issues"][0]["duplicate_of"],
        Value::Null
    );
    // Current persisted categories are four lifecycle values, not wire enums.
    for (key, category, wire) in [
        ("queued_custom", "unstarted", "todo"),
        ("working_custom", "started", "in_progress"),
        ("closed_custom", "closed", "cancelled"),
    ] {
        db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,archived_at) VALUES ($1,$2,'Custom display',$3,'#123456',now())", &[&ws,&key,&category]).await.unwrap();
        db.execute(
            "UPDATE issue SET status=$1 WHERE id=$2",
            &[&key, &issues[0]],
        )
        .await
        .unwrap();
        let (_, page) = call(&app, ws, "snapshot?limit=1", Some(&token), None).await;
        assert_eq!(page["issues"][0]["status_category"], wire);
        assert_eq!(page["issues"][0]["status_name"], "Custom display");
    }
    db.execute(
        "UPDATE issue SET status='missing_custom' WHERE id=$1",
        &[&issues[0]],
    )
    .await
    .unwrap();
    let (_, page) = call(&app, ws, "snapshot?limit=1", Some(&token), None).await;
    assert!(page["issues"][0].get("status_category").is_none());
    assert_eq!(page["issues"][0]["status_name"], "");
    db.execute("UPDATE issue SET status='todo' WHERE id=$1", &[&issues[0]])
        .await
        .unwrap();
    // An in-progress writer is absent from the manifest but must arrive after commit.
    let mut writer = pool.get().await.unwrap();
    let tx = writer.transaction().await.unwrap();
    let late: Uuid = tx
        .query_one(
            insert,
            &[&ws, &4i32, &"Late commit", &"late", &"todo", &user],
        )
        .await
        .unwrap()
        .get(0);
    let (_, baseline) = call(&app, ws, "manifest", Some(&token), None).await;
    assert_eq!(baseline["issue_count"], 3);
    let since = baseline["cursor"].as_str().unwrap().to_owned();
    let (_, before_commit) = call(&app, ws, "snapshot?after_number=3", Some(&token), None).await;
    assert_eq!(before_commit["issues"], json!([]));
    assert_eq!(before_commit["done"], true);
    tx.commit().await.unwrap();
    db.execute(
        "UPDATE project SET title='Project updated' WHERE id=$1",
        &[&project],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE comment SET deleted_at=now() WHERE id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue WHERE id=$1", &[&issues[1]])
        .await
        .unwrap();
    let tx = writer.transaction().await.unwrap();
    let rolled_back: Uuid = tx
        .query_one(
            insert,
            &[&ws, &5i32, &"ROLLED BACK", &"no leak", &"todo", &user],
        )
        .await
        .unwrap()
        .get(0);
    tx.rollback().await.unwrap();
    let mut cursor = since.clone();
    let mut found = HashSet::new();
    let mut deleted = HashSet::new();
    for page in 0..12 {
        let (status, result) = call(
            &app,
            ws,
            "changes",
            Some(&token),
            Some(json!({"cursor":cursor,"limit":1})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert!(!result.to_string().contains("PRIVATE"));
        for key in ["issues", "comments", "projects"] {
            for item in result[key].as_array().unwrap() {
                found.insert(item["id"].as_str().unwrap().to_owned());
            }
        }
        for key in ["issues", "comments", "projects"] {
            for id in result["deleted"][key].as_array().unwrap() {
                deleted.insert(id.as_str().unwrap().to_owned());
            }
        }
        cursor = result["cursor"].as_str().unwrap().to_owned();
        if page == 0 {
            db.execute(
                "UPDATE issue SET title='Changed during paging' WHERE id=$1",
                &[&late],
            )
            .await
            .unwrap();
        }
        if !result["has_more"].as_bool().unwrap() {
            break;
        }
        assert!(page < 11, "bounded pages never completed");
    }
    assert!(found.contains(&late.to_string()));
    assert!(found.contains(&project.to_string()));
    assert!(deleted.contains(&comment.to_string()));
    assert!(deleted.contains(&issues[1].to_string()));
    assert!(!found.contains(&rolled_back.to_string()));
    let (status, next) = call(
        &app,
        ws,
        "changes",
        Some(&token),
        Some(json!({"cursor":cursor,"limit":1000})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next["issues"][0]["title"], "Changed during paging");
    let empty = call(
        &app,
        ws,
        "changes",
        Some(&token),
        Some(json!({"cursor":next["cursor"]})),
    )
    .await
    .1;
    assert_eq!(empty["issues"], json!([]));
    assert_eq!(empty["has_more"], false);
    // Same opaque time cursor on another workspace remains scoped to that tenant.
    let (_, other_result) = call(
        &app,
        other,
        "changes",
        Some(&token),
        Some(json!({"cursor":since})),
    )
    .await;
    assert_eq!(other_result["issues"], json!([]));
    assert_eq!(
        other_result["deleted"],
        json!({"issues":[],"comments":[],"projects":[]})
    );
    for body in [
        json!({}),
        json!({"cursor":"bad"}),
        json!({"cursor":since,"limit":"bad"}),
    ] {
        assert_eq!(
            call(&app, ws, "changes", Some(&token), Some(body)).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let invalid = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({"v":1,"s":"10:1:"})).unwrap());
    assert_eq!(
        call(
            &app,
            other,
            "changes",
            Some(&token),
            Some(json!({"cursor":invalid}))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let decoded: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&since).unwrap()).unwrap();
    let since_snapshot = decoded["s"].as_str().unwrap();
    // Verify index eligibility on the real catalog; this is not a throughput benchmark.
    let tx = writer.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL enable_seqscan=off")
        .await
        .unwrap();
    let target: String = tx
        .query_one("SELECT pg_current_snapshot()::text", &[])
        .await
        .unwrap()
        .get(0);
    let plan: Value = tx.query_one("EXPLAIN (FORMAT JSON) SELECT entity_type,entity_id,change_xid::text FROM search_index_change WHERE workspace_id=$1 AND change_xid>=pg_snapshot_xmin(($2::text)::pg_snapshot) AND change_xid<pg_snapshot_xmax(($3::text)::pg_snapshot) AND NOT pg_visible_in_snapshot(change_xid,($2::text)::pg_snapshot) AND pg_visible_in_snapshot(change_xid,($3::text)::pg_snapshot) AND (change_xid,entity_type,entity_id)>(('0'::text)::xid8,''::text,'00000000-0000-0000-0000-000000000000'::uuid) ORDER BY change_xid,entity_type,entity_id LIMIT 1001", &[&ws,&since_snapshot,&target]).await.unwrap().get(0);
    assert!(
        plan.to_string()
            .contains("idx_search_index_change_workspace_xid"),
        "{plan}"
    );
    let plan: Value = tx.query_one("EXPLAIN (FORMAT JSON) SELECT id FROM issue WHERE workspace_id=$1 AND number>0 ORDER BY number LIMIT 500", &[&ws]).await.unwrap().get(0);
    // The unique constraint and lookup index cover the same ordered columns.
    assert!(
        ["idx_issue_workspace_number", "uq_issue_workspace_number"]
            .iter()
            .any(|name| plan.to_string().contains(name)),
        "{plan}"
    );
    tx.rollback().await.unwrap();
    db.execute("INSERT INTO search_index_prune_mark(singleton,pruned_through_xid) VALUES (true,pg_snapshot_xmin(($1::text)::pg_snapshot)) ON CONFLICT(singleton) DO UPDATE SET pruned_through_xid=excluded.pruned_through_xid",&[&since_snapshot]).await.unwrap();
    assert_eq!(
        call(
            &app,
            ws,
            "changes",
            Some(&token),
            Some(json!({"cursor":since}))
        )
        .await,
        (
            StatusCode::GONE,
            json!({"error":"search index cursor expired; rebuild the local index"})
        )
    );
    assert!(db.query_one("SELECT indisvalid FROM pg_index WHERE indexrelid='idx_search_index_change_workspace_xid'::regclass",&[]).await.unwrap().get::<_,bool>(0));
    db.execute("DELETE FROM search_index_prune_mark", &[])
        .await
        .unwrap();
}
