//! HTTP/PostgreSQL single-axis headers and grouped row branches; no shared DB access.
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
const SECRET: &[u8] = b"queries-sprint-table-groups";
fn token(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
async fn post(
    app: &axum::Router,
    ws: Uuid,
    auth: Option<&str>,
    endpoint: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/api/issues/table/{endpoint}?workspace_id={ws}"))
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
fn request(kind: &str, limit: i64) -> Value {
    json!({"query":{"scope":{"kind":"workspace"},"filters":{},"sort":{"field":"position"}},"group":{"kind":kind},"page":{"limit":limit}})
}
fn compound_request(primary: &str, secondary: &str, format: &str, limit: i64) -> Value {
    let mut req = request("compound", limit);
    req["group"] =
        json!({"kind":"compound","primary":primary,"secondary":secondary,"category_format":format});
    req["query"]["filters"]["statuses"] =
        json!(["todo", "in_progress", "done", "shipped", "archived_state"]);
    req
}
fn keys(body: &Value) -> Vec<String> {
    body["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["key"].as_str().unwrap().to_owned())
        .collect()
}
fn ids(body: &Value) -> Vec<String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["issue"]["id"].as_str().unwrap().to_owned())
        .collect()
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn single_axis_groups_and_grouped_rows() {
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
            "INSERT INTO \"user\"(name,email) VALUES ('Group user',$1) RETURNING id",
            &[&format!("groups-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Group outsider',$1) RETURNING id",
            &[&format!("group-outside-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Groups ws',$1,'GRP') RETURNING id",&[&format!("group-ws-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Groups other',$1,'EXT') RETURNING id",&[&format!("group-other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'same title') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let project2: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'same title') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Group agent','local',$2) RETURNING id",&[&ws,&user]).await.unwrap().get(0);
    let squad:Uuid=db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'Group squad',$2,$3) RETURNING id",&[&ws,&agent,&user]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position) VALUES ($1,'shipped','Shipped','done','#222222',10),($1,'archived_state','Archived state','closed','#222222',1)",&[&ws]).await.unwrap();
    db.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key='archived_state'",
        &[&ws],
    )
    .await
    .unwrap();
    let insert = "INSERT INTO issue(workspace_id,number,title,status,priority,creator_type,creator_id,position,parent_issue_id,assignee_type,assignee_id,project_id) VALUES ($1,$2,$3,$4,'medium','member',$5,$6,$7,$8,$9,$10) RETURNING id";
    let foreign: Uuid = db
        .query_one(
            insert,
            &[
                &other,
                &1i32,
                &"FOREIGN SECRET TITLE",
                &"todo",
                &user,
                &1f64,
                &Option::<Uuid>::None,
                &Option::<String>::None,
                &Option::<Uuid>::None,
                &Option::<Uuid>::None,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let mut issues = Vec::<Uuid>::new();
    for (number, status, parent, kind, assignee, project_id) in [
        (1, "todo", None, Some("member"), Some(user), Some(project)),
        (
            2,
            "todo",
            Some(0usize),
            Some("member"),
            Some(user),
            Some(project),
        ),
        (
            3,
            "in_progress",
            Some(0),
            Some("agent"),
            Some(agent),
            Some(project2),
        ),
        (4, "done", None, Some("squad"), Some(squad), None),
        (5, "shipped", None, None, None, None),
        (6, "archived_state", None, None, None, None),
        (7, "unknown_key", None, None, None, None),
        (8, "todo", None, None, None, None),
    ] {
        let parent_id = parent.map(|i| issues[i]);
        let kind = kind.map(str::to_owned);
        let id = db
            .query_one(
                insert,
                &[
                    &ws,
                    &number,
                    &format!("group issue {number}"),
                    &status,
                    &user,
                    &(number as f64),
                    &parent_id,
                    &kind,
                    &assignee,
                    &project_id,
                ],
            )
            .await
            .unwrap()
            .get(0);
        issues.push(id);
    }
    // Broken historical cross-workspace references must not expose parent context.
    db.execute(
        "UPDATE issue SET parent_issue_id=$2 WHERE id=$1",
        &[&issues[7], &foreign],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "groups".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let auth = token(user);
    let stranger = token(outsider);
    for endpoint in ["groups", "rows"] {
        let mut req = request("status", 50);
        if endpoint == "rows" {
            req["group_key"] = json!("status:todo");
        }
        assert_eq!(
            post(&app, ws, None, endpoint, req.clone()).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            post(&app, ws, Some(&stranger), endpoint, req).await.0,
            StatusCode::NOT_FOUND
        );
    }
    let (status, headers) = post(&app, ws, Some(&auth), "groups", request("status", 50)).await;
    assert_eq!(status, StatusCode::OK, "{headers}");
    assert_eq!(headers["total"], 8);
    assert_eq!(
        keys(&headers),
        [
            "todo",
            "in_progress",
            "done",
            "shipped",
            "archived_state",
            "unknown_key"
        ]
        .map(|s| format!("status:{s}"))
    );
    assert_eq!(headers["groups"][0]["count"], 3);
    assert_eq!(
        headers["groups"][0]["value"],
        json!({"kind":"status","status":"todo","actor":null})
    );
    for kind in ["status", "assignee", "project", "parent"] {
        let mut req = request(kind, 1);
        let mut all = Vec::new();
        let mut first_cursor = None;
        loop {
            let (status, page) = post(&app, ws, Some(&auth), "groups", req.clone()).await;
            assert_eq!(status, StatusCode::OK, "{kind}: {page}");
            assert_eq!(page["total"], 8);
            assert!(!page.to_string().contains("FOREIGN SECRET TITLE"));
            if first_cursor.is_none() {
                first_cursor = page["next_cursor"].as_str().map(str::to_owned);
            }
            for header in page["groups"].as_array().unwrap() {
                let mut row_req = request(kind, 1);
                row_req["group_key"] = header["key"].clone();
                let mut row_ids = Vec::new();
                let mut row_cursor = None;
                loop {
                    let (status, rows) = post(&app, ws, Some(&auth), "rows", row_req.clone()).await;
                    assert_eq!(status, StatusCode::OK, "{kind}: {rows}");
                    assert_eq!(rows["total"], 0);
                    assert_eq!(rows["group_key"], header["key"]);
                    row_ids.extend(ids(&rows));
                    if rows["next_cursor"].is_null() {
                        break;
                    }
                    assert!(row_ids.len() <= 8);
                    row_cursor = rows["next_cursor"].as_str().map(str::to_owned);
                    row_req["page"]["cursor"] = rows["next_cursor"].clone();
                }
                assert_eq!(row_ids.len() as i64, header["count"].as_i64().unwrap());
                let mut unique = row_ids.clone();
                unique.sort();
                unique.dedup();
                assert_eq!(unique.len(), row_ids.len());
                assert!(!row_ids.contains(&foreign.to_string()));
                if let Some(cursor) = row_cursor {
                    row_req["page"]["cursor"] = json!(cursor);
                    row_req["group_key"] = json!("bogus");
                    assert_eq!(
                        post(&app, ws, Some(&auth), "rows", row_req).await.0,
                        StatusCode::CONFLICT
                    );
                }
                all.push(header["key"].as_str().unwrap().to_owned());
            }
            if page["next_cursor"].is_null() {
                break;
            }
            assert!(all.len() <= 8);
            req["page"]["cursor"] = page["next_cursor"].clone();
        }
        let mut unique = all.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), all.len());
        if kind == "assignee" {
            assert_eq!(
                all,
                vec![
                    format!("assignee:member:{user}"),
                    format!("assignee:agent:{agent}"),
                    format!("assignee:squad:{squad}"),
                    "assignee:unassigned".into()
                ]
            );
        }
        if kind == "project" {
            assert_eq!(all[0], "project:none");
            let mut projects = vec![format!("project:{project}"), format!("project:{project2}")];
            projects.sort();
            assert_eq!(&all[1..], projects);
        }
        let mut wrong = request(kind, 1);
        wrong["page"]["cursor"] = json!(first_cursor.unwrap());
        wrong["query"]["filters"]["statuses"] = json!(["todo"]);
        assert_eq!(
            post(&app, ws, Some(&auth), "groups", wrong.clone()).await.0,
            StatusCode::CONFLICT
        );
        wrong["query"]["filters"] = json!({});
        assert_eq!(
            post(&app, other, Some(&auth), "groups", wrong.clone())
                .await
                .0,
            StatusCode::CONFLICT
        );
        wrong["group"]["kind"] = json!(if kind == "status" {
            "project"
        } else {
            "status"
        });
        assert_eq!(
            post(&app, ws, Some(&auth), "groups", wrong).await.0,
            StatusCode::CONFLICT
        );
    }
    let mut filtered = request("assignee", 50);
    filtered["query"]["filters"]["statuses"] = json!(["todo"]);
    let (status, body) = post(&app, ws, Some(&auth), "groups", filtered).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 3);
    assert_eq!(
        keys(&body),
        vec![
            format!("assignee:member:{user}"),
            "assignee:unassigned".into()
        ]
    );
    let mut req = request("status", 50);
    req["query"]["scope"] = json!({"kind":"project","project_id":project});
    let (status, body) = post(&app, ws, Some(&auth), "groups", req).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2);
    let mut req = request("status", 50);
    req["query"]["filters"]["statuses"] = json!(["missing"]);
    let (status, body) = post(&app, ws, Some(&auth), "groups", req).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 0);
    assert!(keys(&body).is_empty());
    let (status, parents) = post(&app, ws, Some(&auth), "groups", request("parent", 50)).await;
    assert_eq!(status, StatusCode::OK, "{parents}");
    let known = parents["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["key"] == format!("parent:{}", issues[0]))
        .unwrap();
    assert_eq!(known["value"]["parent"]["identifier"], "GRP-1");
    assert_eq!(known["value"]["value_state"], "value");
    let unknown = parents["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["key"] == format!("parent:{foreign}"))
        .unwrap();
    assert_eq!(unknown["value"]["value_state"], "unavailable");
    assert!(unknown["value"].get("parent").is_none());
    let mut req = request("status", 50);
    req["group_key"] = json!(" status:todo ");
    req["hierarchy"] = json!({"enabled":true});
    let (status, roots) = post(&app, ws, Some(&auth), "rows", req.clone()).await;
    assert_eq!(status, StatusCode::OK, "{roots}");
    assert_eq!(
        ids(&roots),
        vec![issues[0].to_string(), issues[7].to_string()]
    );
    assert_eq!(roots["rows"][0]["direct_child_count"], 1);
    assert_eq!(roots["group_key"], "status:todo");
    req["parent_id"] = json!(issues[0]);
    let (status, children) = post(&app, ws, Some(&auth), "rows", req.clone()).await;
    assert_eq!(status, StatusCode::OK, "{children}");
    assert_eq!(ids(&children), vec![issues[1].to_string()]);
    req["group_key"] = json!("status:in_progress");
    let (status, body) = post(&app, ws, Some(&auth), "rows", req).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(ids(&body).is_empty());
    let mut req = request("status", 50);
    req["group_key"] = json!("status:in_progress");
    req["hierarchy"] = json!({"enabled":true});
    let (status, body) = post(&app, ws, Some(&auth), "rows", req).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), vec![issues[2].to_string()]);
    for kind in ["status", "assignee", "project", "parent"] {
        for key in [
            "",
            "bogus",
            "status:",
            "assignee:member:bad-uuid",
            "project:bad-uuid",
            "parent:bad-uuid",
        ] {
            let mut req = request(kind, 50);
            req["group_key"] = json!(key);
            assert_eq!(
                post(&app, ws, Some(&auth), "rows", req).await.0,
                StatusCode::BAD_REQUEST
            );
        }
    }
    for (format, expected) in [
        (
            "",
            vec![
                ("todo", 3),
                ("in_progress", 1),
                ("done", 2),
                ("cancelled", 1),
            ],
        ),
        (
            "lifecycle",
            vec![("unstarted", 3), ("started", 1), ("done", 2), ("closed", 1)],
        ),
    ] {
        let mut req = request("status_category", 1);
        req["group"]["category_format"] = json!(format);
        req["query"]["filters"]["statuses"] =
            json!(["todo", "in_progress", "done", "shipped", "archived_state"]);
        let mut found = Vec::new();
        let mut first_cursor = Value::Null;
        loop {
            let (status, headers) = post(&app, ws, Some(&auth), "groups", req.clone()).await;
            assert_eq!(status, StatusCode::OK, "{headers}");
            assert_eq!(headers["total"], 7);
            let group = &headers["groups"][0];
            let key = group["key"].as_str().unwrap();
            let bucket = key.strip_prefix("status_category:").unwrap();
            found.push((bucket.to_owned(), group["count"].as_i64().unwrap()));
            assert_eq!(group["value"]["kind"], "status");
            assert_eq!(group["value"]["status"], bucket);
            let mut rows_req = request("status_category", 1);
            rows_req["group"]["category_format"] = json!(format);
            rows_req["query"]["filters"]["statuses"] = req["query"]["filters"]["statuses"].clone();
            rows_req["group_key"] = group["key"].clone();
            let mut row_ids = Vec::new();
            loop {
                let (status, rows) = post(&app, ws, Some(&auth), "rows", rows_req.clone()).await;
                assert_eq!(status, StatusCode::OK, "{rows}");
                row_ids.extend(ids(&rows));
                if rows["next_cursor"].is_null() {
                    break;
                }
                rows_req["page"]["cursor"] = rows["next_cursor"].clone();
            }
            assert_eq!(row_ids.len() as i64, group["count"].as_i64().unwrap());
            if first_cursor.is_null() {
                first_cursor = headers["next_cursor"].clone();
            }
            if headers["next_cursor"].is_null() {
                break;
            }
            req["page"]["cursor"] = headers["next_cursor"].clone();
        }
        assert_eq!(
            found,
            expected
                .into_iter()
                .map(|(s, c)| (s.to_owned(), c))
                .collect::<Vec<_>>()
        );
        let mut wrong = request("status_category", 1);
        wrong["query"]["filters"]["statuses"] = req["query"]["filters"]["statuses"].clone();
        wrong["group"]["category_format"] = json!(if format.is_empty() { "lifecycle" } else { "" });
        wrong["page"]["cursor"] = first_cursor;
        assert_eq!(
            post(&app, ws, Some(&auth), "groups", wrong).await.0,
            StatusCode::CONFLICT
        );
    }
    let mut invalid_category = request("status_category", 50);
    invalid_category["group_key"] = json!("status_category:unknown");
    assert_eq!(
        post(&app, ws, Some(&auth), "rows", invalid_category)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    for primary in ["assignee", "project", "parent"] {
        for (secondary, format) in [
            ("status", ""),
            ("status_category", ""),
            ("status_category", "lifecycle"),
        ] {
            let mut req = compound_request(primary, secondary, format, 1);
            let mut all_headers = Vec::new();
            loop {
                let (status, headers) = post(&app, ws, Some(&auth), "groups", req.clone()).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{primary}/{secondary}/{format}: {headers}"
                );
                assert_eq!(headers["total"], 7);
                for header in headers["groups"].as_array().unwrap() {
                    let cells = header["secondary_groups"].as_array().unwrap();
                    assert_eq!(
                        cells
                            .iter()
                            .map(|c| c["count"].as_i64().unwrap())
                            .sum::<i64>(),
                        header["count"].as_i64().unwrap()
                    );
                    for cell in cells {
                        assert_eq!(cell["value"]["kind"], "status");
                        let mut rows_req = compound_request(primary, secondary, format, 1);
                        rows_req["group_key"] = cell["key"].clone();
                        let mut row_ids = Vec::new();
                        loop {
                            let (status, rows) =
                                post(&app, ws, Some(&auth), "rows", rows_req.clone()).await;
                            assert_eq!(
                                status,
                                StatusCode::OK,
                                "{primary}/{secondary}/{format}: {rows}"
                            );
                            row_ids.extend(ids(&rows));
                            if rows["next_cursor"].is_null() {
                                break;
                            }
                            rows_req["page"]["cursor"] = rows["next_cursor"].clone();
                        }
                        assert_eq!(row_ids.len() as i64, cell["count"].as_i64().unwrap());
                        assert!(!row_ids.contains(&foreign.to_string()));
                    }
                    all_headers.push(header["key"].as_str().unwrap().to_owned());
                }
                if headers["next_cursor"].is_null() {
                    break;
                }
                req["page"]["cursor"] = headers["next_cursor"].clone();
            }
            let mut unique = all_headers.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), all_headers.len());
            assert_eq!(all_headers.len(), if primary == "assignee" { 4 } else { 3 });
        }
    }
    let mut filtered = compound_request("parent", "status_category", "lifecycle", 50);
    filtered["group"]["secondary_values"] = json!(["todo"]);
    let (status, headers) = post(&app, ws, Some(&auth), "groups", filtered.clone()).await;
    assert_eq!(status, StatusCode::OK, "{headers}");
    assert_eq!(headers["total"], 2);
    let parent_header = headers["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["key"] == format!("parent:{}", issues[0]))
        .unwrap();
    assert_eq!(parent_header["count"], 2);
    assert_eq!(
        parent_header["secondary_groups"][0]["value"]["status"],
        "unstarted"
    );
    let mut rows_req = filtered.clone();
    rows_req["group_key"] = parent_header["secondary_groups"][0]["key"].clone();
    let (status, rows) = post(&app, ws, Some(&auth), "rows", rows_req.clone()).await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(ids(&rows), vec![issues[1].to_string()]);
    rows_req["hierarchy"] = json!({"enabled":true});
    let (status, roots) = post(&app, ws, Some(&auth), "rows", rows_req).await;
    assert_eq!(status, StatusCode::OK, "{roots}");
    assert_eq!(ids(&roots), vec![issues[1].to_string()]);
    filtered["group"]["secondary_values"] = json!([]);
    let (status, empty) = post(&app, ws, Some(&auth), "groups", filtered.clone()).await;
    assert_eq!(status, StatusCode::OK, "{empty}");
    assert!(keys(&empty).is_empty());
    filtered["group"]["secondary_values"] = json!(["todo", "todo"]);
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", filtered).await.0,
        StatusCode::BAD_REQUEST
    );
    for (primary, secondary, code) in [
        ("status", "status", "primary_group_unsupported"),
        ("project", "priority", "secondary_group_unsupported"),
    ] {
        let req = compound_request(primary, secondary, "", 50);
        let (status, body) = post(&app, ws, Some(&auth), "groups", req).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], code);
    }
    for endpoint in ["groups", "rows"] {
        let mut req = request("status", 50);
        if endpoint == "rows" {
            req["group_key"] = json!("status:todo");
        }
        req["group"]["secondary_values"] = json!([]);
        assert_eq!(
            post(&app, ws, Some(&auth), endpoint, req).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", request("none", 50))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, body) = post(&app, ws, Some(&auth), "groups", request("unknown", 50)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "group_kind_unsupported");
    for limit in [-1, 101] {
        assert_eq!(
            post(&app, ws, Some(&auth), "groups", request("status", limit))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let mut req = request("status", 50);
    req["page"]["cursor"] = json!("invalid!");
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut req = request("status", 50);
    req["query"]["unknown"] = json!(true);
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut req = request("status", 50);
    req["query"]["search"] = json!("x".repeat(1 << 20));
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::BAD_REQUEST
    );
    let select: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type,config) VALUES ($1,'Group color','select',$2) RETURNING id", &[&ws,&json!({"options":[{"id":"red","label":"Red"},{"id":"blue","label":"Blue"},{"id":"empty","label":"Empty"}]})]).await.unwrap().get(0);
    let checkbox: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Group checked','checkbox') RETURNING id", &[&ws]).await.unwrap().get(0);
    for (idx, selected, checked) in [
        (0, Some(json!("red")), Some(json!(true))),
        (1, Some(json!("blue")), Some(json!(false))),
        (2, Some(json!("deleted")), Some(json!("true"))),
        (3, Some(Value::Null), Some(Value::Null)),
        (4, Some(json!(42)), Some(json!(42))),
        (5, None, None),
        (6, Some(json!(false)), None),
        (7, None, Some(json!(false))),
    ] {
        let mut properties = json!({});
        if let Some(value) = selected {
            properties[select.to_string()] = value;
        }
        if let Some(value) = checked {
            properties[checkbox.to_string()] = value;
        }
        db.execute(
            "UPDATE issue SET properties=$2 WHERE id=$1",
            &[&issues[idx], &properties],
        )
        .await
        .unwrap();
    }
    for (property, expected) in [
        (
            select,
            vec![
                ("value", "red", 1),
                ("value", "blue", 1),
                ("value", "empty", 0),
                ("unavailable", "", 3),
                ("unavailable", "deleted", 1),
                ("unset", "", 2),
            ],
        ),
        (
            checkbox,
            vec![
                ("value", "false", 2),
                ("value", "true", 1),
                ("unavailable", "", 3),
                ("unset", "", 2),
            ],
        ),
    ] {
        let mut req = request("property", 1);
        req["group"]["property_id"] = json!(property);
        req["group"]["include_empty"] = json!(true);
        let mut actual = Vec::new();
        let mut first_cursor = Value::Null;
        loop {
            let (status, headers) = post(&app, ws, Some(&auth), "groups", req.clone()).await;
            assert_eq!(status, StatusCode::OK, "{headers}");
            assert_eq!(headers["total"], 8);
            let header = &headers["groups"][0];
            actual.push((
                header["key"].as_str().unwrap().to_owned(),
                header["count"].as_i64().unwrap(),
            ));
            assert_eq!(header["value"]["kind"], "property");
            assert_eq!(header["value"]["property_id"], property.to_string());
            let mut rows_req = request("property", 50);
            rows_req["group"] = req["group"].clone();
            rows_req["group_key"] = header["key"].clone();
            let (status, rows) = post(&app, ws, Some(&auth), "rows", rows_req).await;
            assert_eq!(status, StatusCode::OK, "{rows}");
            assert_eq!(ids(&rows).len() as i64, header["count"].as_i64().unwrap());
            if first_cursor.is_null() {
                first_cursor = headers["next_cursor"].clone();
            }
            if headers["next_cursor"].is_null() {
                break;
            }
            req["page"]["cursor"] = headers["next_cursor"].clone();
        }
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|(s, v, c)| (
                    format!("property:{property}:{s}:{}", URL_SAFE_NO_PAD.encode(v)),
                    c
                ))
                .collect::<Vec<_>>()
        );
        req["page"]["cursor"] = first_cursor;
        req["group"]["include_empty"] = json!(false);
        assert_eq!(
            post(&app, ws, Some(&auth), "groups", req.clone()).await.0,
            StatusCode::CONFLICT
        );
        req["page"]["cursor"] = Value::Null;
        req["query"]["filters"]["statuses"] = json!(["missing"]);
        req["group"]["include_empty"] = json!(true);
        let (status, empty) = post(&app, ws, Some(&auth), "groups", req.clone()).await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert_eq!(empty["total"], 0);
        assert_eq!(empty["groups"][0]["count"], 0);
        req["group"]["include_empty"] = json!(false);
        let (status, empty) = post(&app, ws, Some(&auth), "groups", req).await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert!(keys(&empty).is_empty());
    }
    for endpoint in ["groups", "rows"] {
        let mut req = request("property", 50);
        req["group"]["property_id"] = json!(select);
        if endpoint == "rows" {
            req["group_key"] = json!(format!("property:{select}:value:cmVk"));
        }
        let (status, hidden) = post(&app, other, Some(&auth), endpoint, req.clone()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{hidden}");
        assert_eq!(hidden["code"], "property_not_found");
        req["group"]["property_id"] = json!("not-uuid");
        assert_eq!(
            post(&app, ws, Some(&auth), endpoint, req).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let mut invalid_property = request("property", 50);
    invalid_property["group"]["property_id"] = json!(select);
    for key in [
        format!("property:{checkbox}:value:cmVk"),
        format!("property:{select}:value:ZGVsZXRlZA"),
        format!("property:{select}:unset:cmVk"),
        format!("property:{select}:value:!invalid"),
    ] {
        invalid_property["group_key"] = json!(key);
        assert_eq!(
            post(&app, ws, Some(&auth), "rows", invalid_property.clone())
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    db.execute(
        "UPDATE issue_property SET archived_at=now() WHERE id=$1",
        &[&select],
    )
    .await
    .unwrap();
    let (status, archived) = post(&app, ws, Some(&auth), "groups", invalid_property).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "groups rejects group_key field first: {archived}"
    );
    let mut req = request("property", 50);
    req["group"]["property_id"] = json!(select);
    let (status, archived) = post(&app, ws, Some(&auth), "groups", req).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{archived}");
    assert_eq!(archived["code"], "property_archived");
    // Shared cursor identity is checked before endpoint-specific pagination fields.
    let (status, first) = post(&app, ws, Some(&auth), "groups", request("status", 1)).await;
    assert_eq!(status, StatusCode::OK);
    let original: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(first["next_cursor"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    let mut cursor = original.clone();
    cursor["parent_id"] = json!(issues[0]);
    let mut req = request("status", 1);
    req["page"]["cursor"] = json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap()));
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::CONFLICT
    );
    let mut cursor = original.clone();
    cursor.as_object_mut().unwrap().remove("group_sort_key");
    let mut req = request("status", 1);
    req["page"]["cursor"] = json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap()));
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut cursor = original.clone();
    cursor["group_order"] = json!("not-int");
    let mut req = request("status", 1);
    req["page"]["cursor"] = json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).unwrap()));
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut rows_req = request("status", 1);
    rows_req["group_key"] = json!("status:todo");
    rows_req["page"]["cursor"] = first["next_cursor"].clone();
    assert_eq!(
        post(&app, ws, Some(&auth), "rows", rows_req.clone())
            .await
            .0,
        StatusCode::CONFLICT
    );
    rows_req["page"]["cursor"] = Value::Null;
    let (status, row_page) = post(&app, ws, Some(&auth), "rows", rows_req).await;
    assert_eq!(status, StatusCode::OK, "{row_page}");
    assert!(row_page["next_cursor"].is_string());
    let mut req = request("status", 1);
    req["page"]["cursor"] = row_page["next_cursor"].clone();
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", req).await.0,
        StatusCode::CONFLICT
    );
    db.batch_execute("BEGIN; LOCK TABLE issue IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let (status, body) = post(&app, ws, Some(&auth), "groups", request("status", 50)).await;
    db.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");
    assert_eq!(body["error"], "query_timeout");
    assert_eq!(
        post(&app, ws, Some(&auth), "groups", request("status", 50))
            .await
            .0,
        StatusCode::OK
    );
}
