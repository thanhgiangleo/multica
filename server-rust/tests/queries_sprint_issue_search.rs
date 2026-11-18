//! Interactive issue search against disposable PostgreSQL, including cross-tenant search.
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
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-sprint-issue-search";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
fn app(pool: Arc<Pool>) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-issue-search".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    )
}
async fn call(
    app: &axum::Router,
    ws: Uuid,
    query: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!("/api/issues/search?workspace_id={ws}&{query}"));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn search_scope_ranking_snippets_and_pagination() {
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
            &[&format!("issue-search-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Search outsider',$1) RETURNING id",
            &[&format!("issue-search-outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Search A',$1,'QRY') RETURNING id",&[&format!("queries-issue-search-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Search B',$1,'OTH') RETURNING id",&[&format!("queries-issue-search-other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,'shipped','Đã giao','done','#123456')",&[&ws]).await.unwrap();
    let insert = "INSERT INTO issue(workspace_id,number,title,description,status,creator_type,creator_id) VALUES ($1,$2,$3,$4,$5,'member',$6) RETURNING id";
    let title: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &1i32,
                &"alpha beta résumé",
                &"🧪 alpha beta in description",
                &"todo",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let description: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &2i32,
                &"Ordinary",
                &"alpha beta explanation",
                &"in_progress",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let comment: Uuid = db
        .query_one(
            insert,
            &[&ws, &3i32, &"Comment target", &"plain", &"todo", &user],
        )
        .await
        .unwrap()
        .get(0);
    let cancelled: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &4i32,
                &"archived alpha beta",
                &"plain",
                &"cancelled",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let custom: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &5i32,
                &"alpha beta shipped",
                &"plain",
                &"shipped",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=$2",
        &[&title, &cancelled],
    )
    .await
    .unwrap();
    db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'🧪 alpha beta comment') RETURNING id",&[&ws,&comment,&user]).await.unwrap();
    let comment_spread: Uuid = db
        .query_one(
            insert,
            &[&ws, &6i32, &"Spread terms", &"plain", &"todo", &user],
        )
        .await
        .unwrap()
        .get(0);
    for text in ["alpha only", "beta only"] {
        db.execute("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,$4)",&[&ws,&comment_spread,&user,&text]).await.unwrap();
    }
    db.query_one(
        insert,
        &[
            &other,
            &1i32,
            &"alpha beta SECRET",
            &"SECRET",
            &"todo",
            &user,
        ],
    )
    .await
    .unwrap();
    // An old inconsistent comment may point at a different workspace's issue.
    let foreign_comment_target: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &7i32,
                &"Foreign comment target",
                &"plain",
                &"todo",
                &user,
            ],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'FOREIGNSECRET')",
        &[&other, &foreign_comment_target, &user],
    )
    .await
    .unwrap();
    let app = app(pool.clone());
    let token = jwt(user);
    assert_eq!(
        call(&app, ws, "q=alpha", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, ws, "q=alpha", Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, ws, "q=", Some(&token)).await.0,
        StatusCode::BAD_REQUEST
    );
    let (status, response) = call(&app, ws, "q=alpha+beta&include_closed=true", Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let issues = response["issues"].as_array().unwrap();
    let ids: Vec<_> = issues
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids[0], title.to_string());
    assert_eq!(ids[1], custom.to_string());
    assert_eq!(ids[2], description.to_string());
    assert!(
        ids.iter()
            .position(|id| *id == cancelled.to_string())
            .unwrap()
            > ids
                .iter()
                .position(|id| *id == comment.to_string())
                .unwrap()
    );
    assert!(ids.contains(&comment_spread.to_string().as_str()));
    assert!(issues.iter().all(|row| !row.to_string().contains("SECRET")));
    let comment_row = issues
        .iter()
        .find(|row| row["id"] == comment.to_string())
        .unwrap();
    assert_eq!(comment_row["match_source"], "comment");
    assert_eq!(
        comment_row["matched_comment_snippet"],
        "🧪 alpha beta comment"
    );
    assert_eq!(comment_row["matched_snippet"], "🧪 alpha beta comment");
    assert_eq!(
        issues[0]["matched_description_snippet"],
        "🧪 alpha beta in description"
    );
    assert_eq!(issues[1]["status_category"], "done");
    assert_eq!(issues[1]["status_name"], "Đã giao");
    assert!(issues[0].get("search_updated_at").is_none());
    assert!(response.get("total").is_none());
    let (_, page) = call(
        &app,
        ws,
        "q=alpha+beta&include_closed=true&limit=1&offset=2",
        Some(&token),
    )
    .await;
    assert_eq!(page["issues"].as_array().unwrap().len(), 1);
    assert_eq!(page["issues"][0]["id"], description.to_string());
    let (_, closed) = call(&app, ws, "q=alpha+beta", Some(&token)).await;
    assert!(
        !closed["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == cancelled.to_string() || row["id"] == custom.to_string())
    );
    let (status, number) = call(&app, ws, "q=QRY-4&include_closed=true", Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{number}");
    assert_eq!(number["issues"][0]["id"], cancelled.to_string());
    assert!(number["issues"][0]["duplicate_of"].is_object());
    for query in [
        "q=SECRET",
        "q=FOREIGNSECRET",
        "q=alpha%25",
        "q=alpha%5F",
        "q=%27+OR+1%3D1",
    ] {
        let (status, found) = call(&app, ws, query, Some(&token)).await;
        assert_eq!(status, StatusCode::OK, "{query}: {found}");
        assert_eq!(found["issues"], json!([]), "{query}");
    }
}
