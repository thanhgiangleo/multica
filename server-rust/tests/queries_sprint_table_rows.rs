//! HTTP/DB table rows: workspace isolation, keyset pages, hierarchy and negative cases.
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
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-sprint-table-rows";
fn token(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
async fn post(
    app: &axum::Router,
    ws: Uuid,
    auth: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/api/issues/table/rows?workspace_id={ws}"))
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        request = request.header("authorization", format!("Bearer {auth}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("non-JSON {status}: {bytes:?}")),
    )
}
fn request(limit: i64, hierarchy: bool) -> Value {
    json!({"query":{"scope":{"kind":"workspace"},"filters":{},"sort":{"field":"position"}},
        "group":{"kind":"none"},"hierarchy":{"enabled":hierarchy},"page":{"limit":limit}})
}
fn issue_ids(body: &Value) -> Vec<String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["issue"]["id"].as_str().unwrap().to_owned())
        .collect()
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn table_rows_workspace_cursor_hierarchy_and_filters() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
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
            "INSERT INTO \"user\"(name,email) VALUES ('Table user',$1) RETURNING id",
            &[&format!("table-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Table outsider',$1) RETURNING id",
            &[&format!("table-outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Table ws',$1,'TBL') RETURNING id",&[&format!("table-ws-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Table other',$1,'EXT') RETURNING id",&[&format!("table-other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    let label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Table label','#1a2b3c','issue') RETURNING id",&[&ws]).await.unwrap().get(0);
    let insert = "INSERT INTO issue(workspace_id,number,title,status,priority,creator_type,creator_id,position,parent_issue_id) VALUES ($1,$2,$3,$4,'medium','member',$5,$6,$7) RETURNING id";
    let mut issue_ids_fixture = Vec::new();
    for (number, title, status, position, parent) in [
        (1, "alpha root", "todo", 1f64, None),
        (2, "alpha child", "in_progress", 2f64, Some(0usize)),
        (3, "beta root", "done", 3f64, None),
        (4, "beta child", "todo", 4f64, Some(2usize)),
    ] {
        let parent_id: Option<Uuid> = parent.map(|i| issue_ids_fixture[i]);
        let id: Uuid = db
            .query_one(
                insert,
                &[&ws, &number, &title, &status, &user, &position, &parent_id],
            )
            .await
            .unwrap()
            .get(0);
        issue_ids_fixture.push(id);
    }
    let foreign: Uuid = db
        .query_one(
            insert,
            &[
                &other,
                &1i32,
                &"foreign issue",
                &"todo",
                &user,
                &5f64,
                &Option::<Uuid>::None,
            ],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&issue_ids_fixture[0], &label],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "table-rows".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let auth = token(user);
    let unknown = token(outsider);
    assert_eq!(
        post(&app, ws, None, request(2, false)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, ws, Some(&unknown), request(2, false)).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, page1) = post(&app, ws, Some(&auth), request(2, false)).await;
    assert_eq!(status, StatusCode::OK, "{page1}");
    assert_eq!(page1["total"], 4);
    assert_eq!(
        issue_ids(&page1),
        vec![
            issue_ids_fixture[0].to_string(),
            issue_ids_fixture[1].to_string()
        ]
    );
    assert_eq!(page1["rows"][0]["issue"]["identifier"], "TBL-1");
    assert_eq!(
        page1["rows"][0]["issue"]["labels"][0]["id"],
        label.to_string()
    );
    let mut next = request(2, false);
    next["page"]["cursor"] = page1["next_cursor"].clone();
    let (status, page2) = post(&app, ws, Some(&auth), next.clone()).await;
    assert_eq!(status, StatusCode::OK, "{page2}");
    assert_eq!(page2["total"], 0);
    assert_eq!(
        issue_ids(&page2),
        vec![
            issue_ids_fixture[2].to_string(),
            issue_ids_fixture[3].to_string()
        ]
    );
    assert!(page2["next_cursor"].is_null());
    next["query"]["filters"]["statuses"] = json!(["todo"]);
    assert_eq!(
        post(&app, ws, Some(&auth), next).await.0,
        StatusCode::CONFLICT
    );
    let (status, roots) = post(&app, ws, Some(&auth), request(20, true)).await;
    assert_eq!(status, StatusCode::OK, "{roots}");
    assert_eq!(
        issue_ids(&roots),
        vec![
            issue_ids_fixture[0].to_string(),
            issue_ids_fixture[2].to_string()
        ]
    );
    assert_eq!(roots["rows"][0]["direct_child_count"], 1);
    let mut children = request(20, true);
    children["parent_id"] = json!(issue_ids_fixture[0]);
    let (status, child_page) = post(&app, ws, Some(&auth), children.clone()).await;
    assert_eq!(status, StatusCode::OK, "{child_page}");
    assert_eq!(
        issue_ids(&child_page),
        vec![issue_ids_fixture[1].to_string()]
    );
    assert_eq!(child_page["total"], 0);
    children["parent_id"] = json!(foreign);
    let (status, no_foreign) = post(&app, ws, Some(&auth), children).await;
    assert_eq!(status, StatusCode::OK, "{no_foreign}");
    assert!(issue_ids(&no_foreign).is_empty());
    let mut filter = request(20, false);
    filter["query"]["filters"]["label_ids"] = json!([label]);
    let (status, filtered) = post(&app, ws, Some(&auth), filter).await;
    assert_eq!(status, StatusCode::OK, "{filtered}");
    assert_eq!(issue_ids(&filtered), vec![issue_ids_fixture[0].to_string()]);
    let mut invalid = request(20, false);
    invalid["query"]["scope"]["kind"] = json!("project");
    invalid["query"]["scope"]["project_id"] = json!("not-a-uuid");
    assert_eq!(
        post(&app, ws, Some(&auth), invalid).await.0,
        StatusCode::BAD_REQUEST
    );
    for field in ["assignees", "working_issue_ids"] {
        let mut empty = request(20, false);
        empty["query"]["filters"][field] = json!([]);
        let (status, body) = post(&app, ws, Some(&auth), empty).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total"], 0);
        assert!(issue_ids(&body).is_empty());
    }
    for status_filter in ["todo,done", " "] {
        let mut filtered = request(20, false);
        filtered["query"]["filters"]["statuses"] = json!([status_filter]);
        let (status, body) = post(&app, ws, Some(&auth), filtered).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total"], 0);
        assert!(issue_ids(&body).is_empty());
    }
    let mut promoted = request(20, true);
    promoted["query"]["filters"]["statuses"] = json!(["in_progress"]);
    let (status, body) = post(&app, ws, Some(&auth), promoted.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(issue_ids(&body), vec![issue_ids_fixture[1].to_string()]);
    promoted["parent_id"] = json!(issue_ids_fixture[0]);
    let (status, body) = post(&app, ws, Some(&auth), promoted).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(issue_ids(&body).is_empty());
    db.execute("UPDATE issue SET position=1,created_at='2026-10-01T00:00:00Z',updated_at='2026-10-01T00:00:00Z' WHERE workspace_id=$1",&[&ws]).await.unwrap();
    for sort in ["position", "title", "created_at", "updated_at", "priority"] {
        let mut sorted = request(2, false);
        sorted["query"]["sort"]["field"] = json!(sort);
        sorted["query"]["sort"]["direction"] = json!("DESC");
        let (status, first) = post(&app, ws, Some(&auth), sorted.clone()).await;
        assert_eq!(status, StatusCode::OK, "{sort}: {first}");
        let mut cursor: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(first["next_cursor"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        cursor["group_key"] = json!("unexpected-group");
        let mut wrong_group = sorted.clone();
        wrong_group["page"]["cursor"] =
            json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap()));
        assert_eq!(
            post(&app, ws, Some(&auth), wrong_group).await.0,
            StatusCode::CONFLICT
        );
        cursor.as_object_mut().unwrap().remove("group_key");
        if matches!(sort, "created_at" | "updated_at") {
            cursor["sort_value"] = json!("2026-10-01 00:00:00+00");
        }
        sorted["page"]["cursor"] =
            json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap()));
        let (status, second) = post(&app, ws, Some(&auth), sorted.clone()).await;
        assert_eq!(status, StatusCode::OK, "{sort}: {second}");
        let mut all = issue_ids(&first);
        all.extend(issue_ids(&second));
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 4, "{sort}: duplicate or missing rows");
        assert!(second["next_cursor"].is_null());
        assert_eq!(
            post(&app, other, Some(&auth), sorted).await.0,
            StatusCode::CONFLICT
        );
    }
    let mut malformed = request(20, false);
    malformed["query"]["filters"]["surprise"] = json!(true);
    assert_eq!(
        post(&app, ws, Some(&auth), malformed).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut malformed = request(20, false);
    malformed["page"]["cursor"] = json!("not-base64!");
    assert_eq!(
        post(&app, ws, Some(&auth), malformed).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut malformed = request(20, false);
    malformed["query"]["filters"]["date"] = json!({});
    assert_eq!(
        post(&app, ws, Some(&auth), malformed).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut oversized = request(20, false);
    oversized["query"]["search"] = json!("x".repeat(1 << 20));
    assert_eq!(
        post(&app, ws, Some(&auth), oversized).await.0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("BEGIN; LOCK TABLE issue IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        post(&app, ws, Some(&auth), request(20, false)),
    )
    .await;
    db.batch_execute("ROLLBACK").await.unwrap();
    let (status, body) = timeout.expect("table deadline exceeded 12 seconds");
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");
    assert_eq!(body["error"], "query_timeout");
    assert_eq!(
        post(&app, ws, Some(&auth), request(20, false)).await.0,
        StatusCode::OK
    );
    let mut grouped = request(20, false);
    grouped["group"]["kind"] = json!("status");
    assert_eq!(
        post(&app, ws, Some(&auth), grouped.clone()).await.0,
        StatusCode::BAD_REQUEST
    );
    grouped["group_key"] = json!("status:todo");
    let (status, body) = post(&app, ws, Some(&auth), grouped).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["group_key"], "status:todo");
}
