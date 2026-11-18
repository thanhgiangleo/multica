//! Issue detail projection and workspace/security gates on disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        queries_sprint_attachments::{AttachmentRecord, AttachmentUrlPolicy, AttachmentUrls},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-detail-synthetic";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
struct SyntheticUrls;
impl AttachmentUrlPolicy for SyntheticUrls {
    fn resolve(&self, row: &AttachmentRecord, stable: bool) -> Option<AttachmentUrls> {
        Some(AttachmentUrls {
            url: format!("https://safe.invalid/{id}", id = row.id),
            download_url: if stable {
                format!("/api/attachments/{id}/download", id = row.id)
            } else {
                format!("https://safe.invalid/signed/{id}", id = row.id)
            },
            markdown_url: format!(
                "https://safe.invalid/api/attachments/{id}/download",
                id = row.id
            ),
        })
    }
}
fn app_with_urls(pool: Arc<Pool>) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-detail".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()).with_attachment_url_policy(Arc::new(SyntheticUrls)),
    )
}
fn app(pool: Arc<Pool>) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-detail".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    )
}
async fn get(
    app: &axum::Router,
    ws: Uuid,
    issue: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(format!("/api/issues/{issue}/?workspace_id={ws}"));
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn issue_detail_scope_shape_and_fail_closed_unrendered_fields() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432 && *p != 0));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(5)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let nonce = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Detail member',$1) RETURNING id",
            &[&format!("detail-{nonce}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Detail outsider',$1) RETURNING id",
            &[&format!("detail-outsider-{nonce}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let mut workspaces = Vec::new();
    for (name, prefix) in [("local", "LOCAL"), ("foreign", "AWAY")] {
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug,issue_prefix) VALUES ($1,$2,$3) RETURNING id",
                &[&name, &format!("detail-{name}-{nonce}"), &prefix],
            )
            .await
            .unwrap()
            .get(0);
        if name == "local" {
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
                &[&ws, &user],
            )
            .await
            .unwrap();
        }
        workspaces.push(ws);
    }
    let ws = workspaces[0];
    let foreign = workspaces[1];
    let local:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,status,creator_type,creator_id,metadata,properties) VALUES ($1,1,'Detail issue','in_progress','member',$2,'{\"origin\":\"test\"}','{\"x\":true}') RETURNING id",&[&ws,&user]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Private issue','member',$2) RETURNING id",&[&foreign,&user]).await.unwrap().get(0);
    let label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,resource_type,name,color) VALUES ($1,'issue','Owned label','#ffffff') RETURNING id",&[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&local, &label],
    )
    .await
    .unwrap();
    let router = app(pool.clone());
    let token = jwt(user);
    assert_eq!(
        get(&router, ws, &local.to_string(), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&router, ws, &local.to_string(), Some(&jwt(outsider)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, foreign, &local.to_string(), Some(&token))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, ws, &other.to_string(), Some(&token)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, ws, "AWAY-1", Some(&token)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&router, ws, &local.to_string(), Some("invalid"))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for invalid in [
        "invalid",
        "LOCAL-+1",
        "LOCAL--1",
        "LOCAL-0",
        "LOCAL-2147483648",
    ] {
        assert_eq!(
            get(&router, ws, invalid, Some(&token)).await.0,
            StatusCode::NOT_FOUND,
            "{invalid}"
        );
    }
    let (status, body) = get(&router, ws, "local-1", Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["id"], json!(local));
    assert_eq!(body["identifier"], "LOCAL-1");
    assert_eq!(body["status_category"], "in_progress");
    assert_eq!(body["metadata"]["origin"], "test");
    assert_eq!(body["properties"]["x"], true);
    assert_eq!(body["labels"].as_array().unwrap().len(), 1);
    assert_eq!(body["labels"][0]["id"], json!(label));
    assert!(body.get("reactions").is_none());
    assert!(body.get("attachments").is_none());
    assert!(body.get("source_context").is_none());
    assert!(body.get("search_updated_at").is_none());

    let reaction:Uuid=db.query_one("INSERT INTO issue_reaction(workspace_id,issue_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'👍') RETURNING id",&[&ws,&local,&user]).await.unwrap().get(0);
    let (status, body) = get(&router, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["reactions"][0]["id"], json!(reaction));
    assert_eq!(body["reactions"][0]["actor_id"], json!(user));
    assert_eq!(body["reactions"][0]["emoji"], "👍");
    let file: Uuid = db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,'member',$3,'secret.txt','private://object','text/plain',10) RETURNING id",&[&ws,&local,&user]).await.unwrap().get(0);
    let attachment = get(&router, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(attachment.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!attachment.1.to_string().contains("private://object"));
    let signed = app_with_urls(pool.clone());
    let (status, body) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["attachments"][0]["id"], json!(file));
    assert_eq!(
        body["attachments"][0]["download_url"],
        format!("https://safe.invalid/signed/{file}")
    );
    assert_eq!(
        body["attachments"][0]["markdown_url"],
        format!("https://safe.invalid/api/attachments/{file}/download")
    );
    assert!(!body.to_string().contains("private://object"));
    assert_eq!(
        get(&signed, ws, &other.to_string(), Some(&token)).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "DELETE FROM attachment WHERE workspace_id=$1 AND issue_id=$2",
        &[&ws, &local],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_source_context(id,workspace_id,issue_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state,attached_at) VALUES ($1,$2,$3,$3,$4,$5,1,'{}','synthetic','attached',now())",&[&Uuid::new_v4(),&ws,&local,&Uuid::new_v4(),&user]).await.unwrap();
    let context = get(&router, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(context.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(context.1.get("source_context").is_none());
    db.execute(
        "DELETE FROM issue_source_context WHERE workspace_id=$1 AND issue_id=$2",
        &[&ws, &local],
    )
    .await
    .unwrap();

    // Captured root context uses real scoped issue/comment/author rows. Unknown
    // snapshot keys are not reflected, and changed or foreign sources stay closed.
    let source: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,number,title,description,creator_type,creator_id) VALUES ($1,2,'Source title','Source body','member',$2) RETURNING id",
        &[&ws, &user],
    ).await.unwrap().get(0);
    let anchor: Uuid = db.query_one(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'Origin text') RETURNING id",
        &[&ws, &source, &user],
    ).await.unwrap().get(0);
    let issue_row = db
        .query_one(
            "SELECT number,title,description,created_at,updated_at,revision FROM issue WHERE id=$1",
            &[&source],
        )
        .await
        .unwrap();
    let comment_row = db
        .query_one(
            "SELECT created_at,updated_at,revision FROM comment WHERE id=$1",
            &[&anchor],
        )
        .await
        .unwrap();
    let snapshot = json!({
        "version":1,"captured_by_user_id":user,"captured_at":"2026-11-03T00:00:00Z",
        "source_issue":{
            "id":source,"identifier":"LOCAL-2","number":issue_row.get::<_,i32>("number"),
            "title":issue_row.get::<_,String>("title"),"description":issue_row.get::<_,Option<String>>("description"),
            "created_at":issue_row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339(),
            "updated_at":issue_row.get::<_,chrono::DateTime<chrono::Utc>>("updated_at").to_rfc3339(),
            "revision":issue_row.get::<_,i64>("revision"),"attachments":[]
        },
        "comment_thread":[{
            "id":anchor,"parent_id":null,"type":"comment","content":"Origin text",
            "author":{"type":"member","id":user,"name":"Detail member"},
            "created_at":comment_row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339(),
            "updated_at":comment_row.get::<_,chrono::DateTime<chrono::Utc>>("updated_at").to_rfc3339(),
            "revision":comment_row.get::<_,i64>("revision"),"attachments":[]
        }],"anchor_comment_id":anchor,"private_extra":"must_not_echo"
    });
    let context_id = Uuid::new_v4();
    db.execute("INSERT INTO issue_source_context(id,workspace_id,issue_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state,attached_at) VALUES ($1,$2,$3,$4,$5,$6,1,$7,'synthetic','attached',now())",
        &[&context_id,&ws,&local,&source,&anchor,&user,&snapshot]).await.unwrap();
    let mut historical_url = snapshot.clone();
    historical_url["comment_thread"][0]["content"] =
        json!("[private](/api/attachments/00000000-0000-0000-0000-000000000001/download)");
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&historical_url, &context_id],
    )
    .await
    .unwrap();
    let (status, rejected) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{rejected:?}");
    assert!(
        !rejected
            .to_string()
            .contains("00000000-0000-0000-0000-000000000001")
    );
    let mut too_large = snapshot.clone();
    too_large["comment_thread"][0]["content"] = json!("a".repeat((1 << 20) + 1));
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&too_large, &context_id],
    )
    .await
    .unwrap();
    let (status, rejected) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{rejected:?}");
    assert!(rejected.get("source_context").is_none());
    let mut oversized_attachment = snapshot.clone();
    oversized_attachment["source_issue"]["attachments"] = json!([{
        "id":Uuid::new_v4(),"owner_type":"issue","owner_id":source,
        "filename":"large.bin","content_type":"application/octet-stream",
        "size_bytes":(500_i64 << 20) + 1,"created_at":"2026-11-03T00:00:00Z"
    }]);
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&oversized_attachment, &context_id],
    )
    .await
    .unwrap();
    let (status, rejected) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{rejected:?}");
    assert!(rejected.get("source_context").is_none());
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&snapshot, &context_id],
    )
    .await
    .unwrap();
    db.execute("UPDATE comment SET content='[private](/api/attachments/00000000-0000-0000-0000-000000000002/download)' WHERE id=$1", &[&anchor]).await.unwrap();
    assert_eq!(
        get(&signed, ws, &local.to_string(), Some(&token)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE comment SET content='Origin text' WHERE id=$1",
        &[&anchor],
    )
    .await
    .unwrap();
    db.execute("UPDATE issue SET description='[private](/api/attachments/00000000-0000-0000-0000-000000000003/download)' WHERE id=$1", &[&source]).await.unwrap();
    assert_eq!(
        get(&signed, ws, &local.to_string(), Some(&token)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE issue SET description='Source body' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    let (status, rendered) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{rendered:?}");
    let context = &rendered["source_context"];
    assert_eq!(context["id"], json!(context_id));
    assert_eq!(context["usage"], "read_only_historical_background");
    assert_eq!(context["display_state"], "unchanged");
    assert_eq!(
        context["current_source"]["anchor_comment_id"],
        json!(anchor)
    );
    assert_eq!(context["source_author_state"][0]["state"], "unchanged");
    assert!(context["snapshot"].get("private_extra").is_none());

    // Capturing a reply includes its root and all chronological predecessors,
    // including a reply at the same timestamp whose UUID sorts before anchor.
    let boundary = chrono::Utc::now() - chrono::Duration::minutes(1);
    db.execute(
        "UPDATE comment SET created_at=$1 WHERE id=$2",
        &[&(boundary - chrono::Duration::minutes(2)), &anchor],
    )
    .await
    .unwrap();
    let first: Uuid = db.query_one("INSERT INTO comment(id,workspace_id,issue_id,parent_id,author_type,author_id,content,created_at) VALUES ($1,$2,$3,$4,'member',$5,'First reply',$6) RETURNING id", &[&Uuid::from_u128(3),&ws,&source,&anchor,&outsider,&boundary]).await.unwrap().get(0);
    let last: Uuid = db.query_one("INSERT INTO comment(id,workspace_id,issue_id,parent_id,author_type,author_id,content,created_at) VALUES ($1,$2,$3,$4,'member',$5,'Captured reply',$6) RETURNING id", &[&Uuid::from_u128(4),&ws,&source,&first,&user,&boundary]).await.unwrap().get(0);
    let later: Uuid = db.query_one("INSERT INTO comment(workspace_id,issue_id,parent_id,author_type,author_id,content,created_at) VALUES ($1,$2,$3,'member',$4,'Later reply',$5) RETURNING id", &[&ws,&source,&anchor,&user,&(boundary+chrono::Duration::minutes(1))]).await.unwrap().get(0);
    let mut thread_snapshot = snapshot.clone();
    thread_snapshot["comment_thread"][0]["created_at"] =
        json!((boundary - chrono::Duration::minutes(2)).to_rfc3339());
    for (id, parent, text) in [
        (first, anchor, "First reply"),
        (last, first, "Captured reply"),
    ] {
        let row = db
            .query_one(
                "SELECT created_at,updated_at,revision FROM comment WHERE id=$1",
                &[&id],
            )
            .await
            .unwrap();
        thread_snapshot["comment_thread"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id":id,"parent_id":parent,"type":"comment","content":text,
                "author":{"type":"member","id":if id == first { outsider } else { user },
                    "name":if id == first { "Detail outsider" } else { "Detail member" }},
                "created_at":row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339(),
                "updated_at":row.get::<_,chrono::DateTime<chrono::Utc>>("updated_at").to_rfc3339(),
                "revision":row.get::<_,i64>("revision"),"attachments":[]
            }));
    }
    thread_snapshot["anchor_comment_id"] = json!(last);
    db.execute(
        "UPDATE issue_source_context SET anchor_comment_id=$1,snapshot=$2 WHERE id=$3",
        &[&last, &thread_snapshot, &context_id],
    )
    .await
    .unwrap();
    let (status, initial_thread) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{initial_thread:?}");
    assert_eq!(
        initial_thread["source_context"]["display_state"],
        "unchanged"
    );
    assert_eq!(
        initial_thread["source_context"]["snapshot"]["comment_thread"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let author_states = initial_thread["source_context"]["source_author_state"]
        .as_array()
        .unwrap();
    assert_eq!(author_states.len(), 2);
    assert_eq!(author_states[0]["state"], "unchanged");
    assert_eq!(author_states[1]["state"], "no_longer_in_workspace");
    assert!(author_states[1].get("current_name").is_none());
    assert!(
        !initial_thread["source_context"]
            .to_string()
            .contains(&later.to_string())
    );
    let discovered:Uuid=db.query_one("INSERT INTO comment(id,workspace_id,issue_id,parent_id,author_type,author_id,content,created_at) VALUES ($1,$2,$3,$4,'member',$5,'Earlier same-time reply',$6) RETURNING id", &[&Uuid::from_u128(2),&ws,&source,&anchor,&outsider,&boundary]).await.unwrap().get(0);
    let (_, added_thread) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(
        added_thread["source_context"]["change_details"]["added_comments"][0]["id"],
        json!(discovered)
    );
    assert_eq!(
        added_thread["source_context"]["change_details"]["added_comments"][0]["author"]["name"],
        "Detail outsider"
    );
    assert_eq!(
        added_thread["source_context"]["change_details"]["added_comments"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let batch_time = boundary - chrono::Duration::seconds(1);
    db.execute(
        "WITH new_authors AS (INSERT INTO \"user\"(name,email) \
         SELECT 'Batch author ' || n, 'batch-' || $1::text || '-' || n || '@invalid.example' \
         FROM generate_series(1,80) n RETURNING id,name) \
         INSERT INTO comment(workspace_id,issue_id,parent_id,author_type,author_id,content,created_at) \
         SELECT $2,$3,$4,'member',id,'Batch reply', $5 FROM new_authors",
        &[&nonce.to_string(),&ws,&source,&anchor,&batch_time],
    ).await.unwrap();
    let (batch_status, batch) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(batch_status, StatusCode::OK, "{batch:?}");
    let added = batch["source_context"]["change_details"]["added_comments"]
        .as_array()
        .unwrap();
    assert_eq!(added.len(), 81);
    let batch_names: std::collections::HashSet<_> = added
        .iter()
        .filter(|comment| comment["content"] == "Batch reply")
        .map(|comment| comment["author"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(batch_names.len(), 80);
    assert!(batch_names.contains("Batch author 80"));
    db.execute(
        "DELETE FROM comment WHERE issue_id=$1 AND content='Batch reply'",
        &[&source],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM comment WHERE id=$1", &[&discovered])
        .await
        .unwrap();
    db.execute(
        "UPDATE comment SET content='Edited reply' WHERE id=$1",
        &[&first],
    )
    .await
    .unwrap();
    let (_, edited_thread) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(
        edited_thread["source_context"]["change_details"]["changed_comment_ids"],
        json!([first])
    );
    db.execute("UPDATE comment SET deleted_at=now() WHERE id=$1", &[&first])
        .await
        .unwrap();
    let (_, removed_thread) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(
        removed_thread["source_context"]["change_details"]["removed_comment_ids"],
        json!([first])
    );
    assert_eq!(
        removed_thread["source_context"]["anchor_comment_state"],
        "available"
    );
    db.execute(
        "UPDATE comment SET deleted_at=NULL,content='First reply' WHERE id=$1",
        &[&first],
    )
    .await
    .unwrap();
    db.execute("UPDATE comment SET deleted_at=now() WHERE id=$1", &[&last])
        .await
        .unwrap();
    let (_, missing_anchor) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(
        missing_anchor["source_context"]["anchor_comment_state"],
        "deleted"
    );
    assert_eq!(
        missing_anchor["source_context"]["change_details"]["removed_comment_ids"],
        json!([last])
    );
    db.execute("UPDATE comment SET deleted_at=NULL WHERE id=$1", &[&last])
        .await
        .unwrap();
    db.execute(
        "UPDATE issue_source_context SET anchor_comment_id=$1,snapshot=$2 WHERE id=$3",
        &[&anchor, &snapshot, &context_id],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM comment WHERE id=ANY($1)",
        &[&vec![later, last, first]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE comment SET content='changed' WHERE id=$1",
        &[&anchor],
    )
    .await
    .unwrap();
    let (status, edited) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{edited:?}");
    assert_eq!(edited["source_context"]["display_state"], "changed");
    assert_eq!(
        edited["source_context"]["change_reasons"],
        json!(["comment_thread"])
    );
    assert_eq!(
        edited["source_context"]["change_details"]["changed_comment_ids"],
        json!([anchor])
    );
    assert_eq!(
        edited["source_context"]["snapshot"]["comment_thread"][0]["content"],
        "Origin text"
    );
    db.execute(
        "UPDATE comment SET content='Origin text' WHERE id=$1",
        &[&anchor],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE \"user\" SET name='Renamed author' WHERE id=$1",
        &[&user],
    )
    .await
    .unwrap();
    let (status, renamed) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{renamed:?}");
    assert_eq!(renamed["source_context"]["display_state"], "unchanged");
    assert_eq!(
        renamed["source_context"]["source_author_state"][0]["state"],
        "renamed"
    );
    assert_eq!(
        renamed["source_context"]["source_author_state"][0]["current_name"],
        "Renamed author"
    );
    db.execute(
        "UPDATE \"user\" SET name='Detail member' WHERE id=$1",
        &[&user],
    )
    .await
    .unwrap();
    let source_attachment: Uuid = db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,'member',$3,'private.bin','private://unavailable','application/octet-stream',4) RETURNING id", &[&ws,&source,&user]).await.unwrap().get(0);
    let safe_inventory = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(safe_inventory.0, StatusCode::OK);
    assert!(
        !safe_inventory
            .1
            .to_string()
            .contains("private://unavailable")
    );
    assert_eq!(
        safe_inventory.1["source_context"]["change_reasons"],
        json!([])
    );
    let clone_id = Uuid::new_v4();
    let mut with_link = snapshot.clone();
    with_link["source_issue"]["description"] = json!(format!(
        "Source body\n\n![private.bin](/api/attachments/{clone_id}/download)"
    ));
    with_link["source_issue"]["attachments"] = json!([{
        "id":clone_id,"source_attachment_id":source_attachment,"owner_type":"issue","owner_id":source,
        "filename":"private.bin","content_type":"application/octet-stream","size_bytes":4,
        "created_at":comment_row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339()
    }]);
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&with_link, &context_id],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue SET description=$1 WHERE id=$2",
        &[
            &format!(
                "Source body\n\n![private.bin](/api/attachments/{source_attachment}/download)"
            ),
            &source,
        ],
    )
    .await
    .unwrap();
    let (status, same_link) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{same_link:?}");
    assert_eq!(same_link["source_context"]["change_reasons"], json!([]));
    assert_eq!(
        same_link["source_context"]["change_details"]["description_attachment_changes"],
        json!([])
    );
    db.execute(
        "UPDATE issue SET description='Source body' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    let (status, missing_link) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{missing_link:?}");
    assert_eq!(
        missing_link["source_context"]["change_reasons"],
        json!(["issue_description_attachments"])
    );
    assert_eq!(
        missing_link["source_context"]["change_details"]["description_attachment_changes"],
        json!([{"kind":"removed","attachment_id":source_attachment,"filename":"private.bin"}])
    );
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&snapshot, &context_id],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM attachment WHERE id=$1", &[&source_attachment])
        .await
        .unwrap();

    // Live metadata can exceed the capture budget without invalidating the
    // historical snapshot. Compare it, but never expose private storage URLs.
    db.execute(
        "INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         SELECT $1,$2,$3,'member',$4,'bulk-' || n,'private://bulk','text/plain',1 \
         FROM generate_series(1,101) AS n",
        &[&ws, &source, &anchor, &user],
    ).await.unwrap();
    let oversized = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(oversized.0, StatusCode::OK);
    assert_eq!(oversized.1["source_context"]["display_state"], "changed");
    assert_eq!(
        oversized.1["source_context"]["source_issue_state"],
        "unchanged"
    );
    assert_eq!(
        oversized.1["source_context"]["comment_thread_state"],
        "changed"
    );
    assert_eq!(
        oversized.1["source_context"]["change_details"]["changed_comment_ids"],
        json!([anchor])
    );
    assert!(!oversized.1.to_string().contains("private://bulk"));
    db.execute(
        "DELETE FROM attachment WHERE issue_id=$1 AND comment_id=$2",
        &[&source, &anchor],
    )
    .await
    .unwrap();

    db.execute(
        "INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         SELECT $1,$2,'member',$3,'issue-' || n,'private://issue-bulk','text/plain',1 \
         FROM generate_series(1,60) AS n",
        &[&ws,&source,&user],
    ).await.unwrap();
    db.execute(
        "INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         SELECT $1,$2,$3,'member',$4,'thread-' || n,'private://thread-bulk','text/plain',1 \
         FROM generate_series(1,41) AS n",
        &[&ws,&source,&anchor,&user],
    ).await.unwrap();
    let (status, combined) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{combined:?}");
    assert_eq!(combined["source_context"]["display_state"], "changed");
    assert_eq!(
        combined["source_context"]["source_issue_state"],
        "unchanged"
    );
    assert_eq!(
        combined["source_context"]["comment_thread_state"],
        "changed"
    );
    assert!(!combined.to_string().contains("private://"));
    db.execute("DELETE FROM attachment WHERE issue_id=$1", &[&source])
        .await
        .unwrap();
    let huge: Uuid = db.query_one(
        "INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         VALUES ($1,$2,'member',$3,'too-large.bin','private://large','application/octet-stream',$4) RETURNING id",
        &[&ws,&source,&user,&((500_i64 << 20) + 1)],
    ).await.unwrap().get(0);
    let (status, huge_body) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{huge_body:?}");
    assert_eq!(huge_body["source_context"]["display_state"], "unchanged");
    assert_eq!(huge_body["source_context"]["can_open_current_source"], true);
    assert!(!huge_body.to_string().contains("private://large"));
    db.execute("DELETE FROM attachment WHERE id=$1", &[&huge])
        .await
        .unwrap();

    db.execute(
        "INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         SELECT $1,$2,'member',$3,'excess-' || n,'private://excess','text/plain',1 \
         FROM generate_series(1,1024) AS n",
        &[&ws,&source,&user],
    ).await.unwrap();
    let (status, at_limit) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{at_limit:?}");
    assert_eq!(at_limit["source_context"]["display_state"], "unchanged");
    assert_eq!(at_limit["source_context"]["can_open_current_source"], true);
    assert!(!at_limit.to_string().contains("private://excess"));
    db.execute(
        "INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         VALUES ($1,$2,$3,'member',$4,'excess-thread','private://excess-thread','text/plain',1)",
        &[&ws,&source,&anchor,&user],
    ).await.unwrap();
    let (status, excess) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{excess:?}");
    assert_eq!(excess["source_context"]["display_state"], "unavailable");
    assert_eq!(excess["source_context"]["can_open_current_source"], false);
    assert!(!excess.to_string().contains("private://excess"));
    db.execute("DELETE FROM attachment WHERE issue_id=$1", &[&source])
        .await
        .unwrap();

    let mut with_attachment = snapshot.clone();
    let comment_attachment: Uuid = db.query_one("INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,$3,'member',$4,'thread.bin','private://thread-key','application/octet-stream',7) RETURNING id", &[&ws,&source,&anchor,&user]).await.unwrap().get(0);
    let metadata = json!({"id":comment_attachment,"owner_type":"comment","owner_id":anchor,
        "filename":"thread.bin","content_type":"application/octet-stream","size_bytes":7,
        "created_at":comment_row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339()});
    with_attachment["comment_thread"][0]["attachments"] = json!([metadata]);
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&with_attachment, &context_id],
    )
    .await
    .unwrap();
    let unchanged = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(unchanged.0, StatusCode::OK, "{:?}", unchanged.1);
    assert_eq!(
        unchanged.1["source_context"]["comment_thread_state"],
        "unchanged"
    );
    assert!(!unchanged.1.to_string().contains("private://thread-key"));
    db.execute(
        "UPDATE attachment SET filename='renamed.bin' WHERE id=$1",
        &[&comment_attachment],
    )
    .await
    .unwrap();
    let renamed_file = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(renamed_file.0, StatusCode::OK, "{:?}", renamed_file.1);
    assert_eq!(
        renamed_file.1["source_context"]["change_details"]["changed_comment_ids"],
        json!([anchor])
    );
    assert_eq!(
        renamed_file.1["source_context"]["change_reasons"],
        json!(["comment_thread"])
    );
    db.execute(
        "UPDATE attachment SET filename='thread.bin' WHERE id=$1",
        &[&comment_attachment],
    )
    .await
    .unwrap();
    // A captured comment that links the same attachment through a cloned URL
    // compares unchanged; only the durable identity participates.
    let thread_clone = Uuid::new_v4();
    let mut cloned_comment = snapshot.clone();
    cloned_comment["comment_thread"][0]["content"] = json!(format!(
        "Origin [thread.bin](/api/attachments/{thread_clone}/download) text"
    ));
    cloned_comment["comment_thread"][0]["attachments"] = json!([{
        "id":thread_clone,"source_attachment_id":comment_attachment,"owner_type":"comment","owner_id":anchor,
        "filename":"thread.bin","content_type":"application/octet-stream","size_bytes":7,
        "created_at":comment_row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339()
    }]);
    db.execute(
        "UPDATE comment SET content=$1 WHERE id=$2",
        &[
            &format!("Origin [thread.bin](/api/attachments/{comment_attachment}/download) text"),
            &anchor,
        ],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&cloned_comment, &context_id],
    )
    .await
    .unwrap();
    let (status, linked_comment) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{linked_comment:?}");
    assert_eq!(
        linked_comment["source_context"]["comment_thread_state"],
        "unchanged"
    );
    assert_eq!(
        linked_comment["source_context"]["change_reasons"],
        json!([])
    );
    db.execute("UPDATE comment SET content='Origin [thread.bin](/api/attachments/00000000-0000-0000-0000-000000000004/download) text' WHERE id=$1",&[&anchor]).await.unwrap();
    let (status, foreign_comment) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{foreign_comment:?}"
    );
    db.execute(
        "UPDATE comment SET content='Origin text' WHERE id=$1",
        &[&anchor],
    )
    .await
    .unwrap();
    let mut cloned = with_attachment.clone();
    cloned["comment_thread"][0]["attachments"][0]["id"] = json!(Uuid::new_v4());
    cloned["comment_thread"][0]["attachments"][0]["source_attachment_id"] =
        json!(comment_attachment);
    db.execute(
        "UPDATE attachment SET filename='thread.bin' WHERE id=$1",
        &[&comment_attachment],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&cloned, &context_id],
    )
    .await
    .unwrap();
    let alias = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(alias.0, StatusCode::OK, "{:?}", alias.1);
    assert_eq!(
        alias.1["source_context"]["comment_thread_state"],
        "unchanged"
    );
    cloned["comment_thread"][0]["attachments"][0]["owner_id"] = json!(other);
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&cloned, &context_id],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&signed, ws, &local.to_string(), Some(&token)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&with_attachment, &context_id],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM attachment WHERE id=$1", &[&comment_attachment])
        .await
        .unwrap();
    let removed_file = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(removed_file.0, StatusCode::OK, "{:?}", removed_file.1);
    assert_eq!(
        removed_file.1["source_context"]["change_details"]["changed_comment_ids"],
        json!([anchor])
    );
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&snapshot, &context_id],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_source_context SET source_issue_id=$1 WHERE id=$2",
        &[&other, &context_id],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&signed, ws, &local.to_string(), Some(&token)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE issue_source_context SET source_issue_id=$1 WHERE id=$2",
        &[&source, &context_id],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&signed, ws, &local.to_string(), Some(&token)).await.0,
        StatusCode::OK
    );
    // Identity labels are live metadata; do not mark a snapshot changed when
    // only the source issue number or workspace prefix changes.
    db.execute("UPDATE issue SET number=3 WHERE id=$1", &[&source])
        .await
        .unwrap();
    let (status, renumbered) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{renumbered:?}");
    assert_eq!(renumbered["source_context"]["display_state"], "unchanged");
    assert_eq!(renumbered["source_context"]["change_reasons"], json!([]));
    assert_eq!(
        renumbered["source_context"]["current_source"]["identifier"],
        "LOCAL-3"
    );
    assert_eq!(
        renumbered["source_context"]["snapshot"]["source_issue"]["identifier"],
        "LOCAL-2"
    );
    db.execute(
        "UPDATE workspace SET issue_prefix='RENAMED' WHERE id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    let (status, prefix) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{prefix:?}");
    assert_eq!(prefix["source_context"]["display_state"], "unchanged");
    assert_eq!(
        prefix["source_context"]["current_source"]["identifier"],
        "RENAMED-3"
    );
    db.execute(
        "UPDATE workspace SET issue_prefix='LOCAL' WHERE id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    db.execute("UPDATE issue SET number=2 WHERE id=$1", &[&source])
        .await
        .unwrap();
    db.execute(
        "UPDATE issue SET title='Changed source' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    let (status, changed) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{changed:?}");
    assert_eq!(changed["source_context"]["display_state"], "changed");
    assert_eq!(
        changed["source_context"]["change_reasons"],
        json!(["issue_title"])
    );
    db.execute(
        "UPDATE issue SET title='Source title',description='Updated body' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    let (status, changed_body) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{changed_body:?}");
    assert_eq!(
        changed_body["source_context"]["change_reasons"],
        json!(["issue_description"])
    );
    db.execute(
        "UPDATE issue SET description='Source body' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE comment SET deleted_at=now() WHERE id=$1",
        &[&anchor],
    )
    .await
    .unwrap();
    let (status, deleted) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{deleted:?}");
    assert_eq!(deleted["source_context"]["display_state"], "changed");
    assert_eq!(deleted["source_context"]["anchor_comment_state"], "deleted");
    assert_eq!(
        deleted["source_context"]["change_details"]["removed_comment_ids"],
        json!([anchor])
    );
    assert!(deleted["source_context"].get("current_source").is_none());
    db.execute("UPDATE comment SET deleted_at=NULL WHERE id=$1", &[&anchor])
        .await
        .unwrap();
    let agent: Uuid = db.query_one(
        "INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Captured agent','local',$2) RETURNING id",
        &[&ws, &user],
    ).await.unwrap().get(0);
    db.execute(
        "UPDATE comment SET author_type='agent',author_id=$1 WHERE id=$2",
        &[&agent, &anchor],
    )
    .await
    .unwrap();
    let mut agent_snapshot = snapshot.clone();
    agent_snapshot["comment_thread"][0]["author"] =
        json!({"type":"agent","id":agent,"name":"Captured agent"});
    db.execute(
        "UPDATE issue_source_context SET snapshot=$1 WHERE id=$2",
        &[&agent_snapshot, &context_id],
    )
    .await
    .unwrap();
    let (status, agent_body) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{agent_body:?}");
    assert_eq!(
        agent_body["source_context"]["source_author_state"][0]["state"],
        "unchanged"
    );
    db.execute(
        "UPDATE agent SET name='New agent', archived_at=now() WHERE id=$1",
        &[&agent],
    )
    .await
    .unwrap();
    let (status, archived) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    assert_eq!(archived["source_context"]["display_state"], "unchanged");
    assert_eq!(
        archived["source_context"]["source_author_state"][0]["state"],
        "archived"
    );
    db.execute("DELETE FROM agent WHERE id=$1", &[&agent])
        .await
        .unwrap();
    let (status, missing) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{missing:?}");
    assert_eq!(
        missing["source_context"]["source_author_state"][0]["state"],
        "deleted_agent"
    );
    assert!(
        missing["source_context"]["source_author_state"][0]
            .get("current_name")
            .is_none()
    );
    db.execute("DELETE FROM comment WHERE id=$1", &[&anchor])
        .await
        .unwrap();
    let (status, removed_root) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{removed_root:?}");
    assert_eq!(
        removed_root["source_context"]["source_issue_state"],
        "unchanged"
    );
    assert_eq!(
        removed_root["source_context"]["comment_thread_state"],
        "changed"
    );
    assert_eq!(
        removed_root["source_context"]["anchor_comment_state"],
        "deleted"
    );
    assert_eq!(
        removed_root["source_context"]["change_details"]["removed_comment_ids"],
        json!([anchor])
    );
    assert_eq!(
        removed_root["source_context"]["can_open_current_source"],
        false
    );
    assert!(
        removed_root["source_context"]
            .get("current_source")
            .is_none()
    );
    db.execute(
        "DELETE FROM issue WHERE id=$1 AND workspace_id=$2",
        &[&source, &ws],
    )
    .await
    .unwrap();
    let (status, removed_source) = get(&signed, ws, &local.to_string(), Some(&token)).await;
    assert_eq!(status, StatusCode::OK, "{removed_source:?}");
    assert_eq!(removed_source["source_context"]["display_state"], "deleted");
    assert_eq!(
        removed_source["source_context"]["source_issue_state"],
        "deleted"
    );
    assert_eq!(
        removed_source["source_context"]["comment_thread_state"],
        "unavailable"
    );
    assert_eq!(
        removed_source["source_context"]["change_reasons"],
        json!([])
    );
    assert!(
        removed_source["source_context"]["change_details"]
            .get("removed_comment_ids")
            .is_none()
    );
    assert_eq!(
        removed_source["source_context"]["can_open_current_source"],
        false
    );
    assert!(
        removed_source["source_context"]
            .get("current_source")
            .is_none()
    );
    db.execute(
        "DELETE FROM issue_source_context WHERE id=$1",
        &[&context_id],
    )
    .await
    .unwrap();

    let previous_revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&local])
        .await
        .unwrap()
        .get(0);
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let app = router.clone();
        let token = token.clone();
        let id = local.to_string();
        requests.spawn(async move { get(&app, ws, &id, Some(&token)).await });
    }
    while let Some(response) = requests.join_next().await {
        let (status, body) = response.unwrap();
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["id"], json!(local));
        assert_eq!(body["reactions"].as_array().unwrap().len(), 1);
    }
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&local])
            .await
            .unwrap()
            .get::<_, i64>(0),
        previous_revision
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*)::bigint FROM issue WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}
