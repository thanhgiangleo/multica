//! Invitation settlement HTTP/DB checks on a disposable database.
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

const SECRET: &str = "synthetic-zbsi65-invitation-secret";
fn jwt(user: Uuid, seconds: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+seconds}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn call(app: &axum::Router, method: &str, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}
fn database() -> Config {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    cfg
}
fn app(pool: Arc<Pool>, cloud: bool) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec());
    if cloud {
        state = state.with_cloud_url("https://example.invalid").unwrap();
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
struct Fixture {
    app: axum::Router,
    pool: Arc<Pool>,
    ws: Uuid,
    owner: Uuid,
    admin: Uuid,
    member: Uuid,
    invitee: Uuid,
    outsider: Uuid,
}
impl Fixture {
    async fn new(cloud: bool) -> Self {
        let pool = Arc::new(
            Pool::builder(Manager::new(database(), NoTls))
                .max_size(8)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let mut users = Vec::new();
        for name in ["Owner", "Admin", "Member", "Invitee", "Outsider"] {
            let row = db
                .query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[
                        &name,
                        &format!("{}-{suffix}@invalid.example", name.to_lowercase()),
                    ],
                )
                .await
                .unwrap();
            users.push(row.get(0));
        }
        let [owner, admin, member, invitee, outsider] = <[Uuid; 5]>::try_from(users).unwrap();
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Invitations',$1) RETURNING id",
                &[&format!("invitations-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        for (user, role) in [(owner, "owner"), (admin, "admin"), (member, "member")] {
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
                &[&ws, &user, &role],
            )
            .await
            .unwrap();
        }
        Self {
            app: app(pool.clone(), cloud),
            pool,
            ws,
            owner,
            admin,
            member,
            invitee,
            outsider,
        }
    }
    async fn invitation(&self, user: Uuid, expires: &str) -> Uuid {
        let db = self.pool.get().await.unwrap();
        let email: String = db
            .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&user])
            .await
            .unwrap()
            .get(0);
        db.query_one(
            "INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES($1,$2,$3,$4,'member',now()+$5::text::interval) RETURNING id",
            &[&self.ws, &self.owner, &email, &user, &expires],
        )
        .await
        .unwrap()
        .get(0)
    }
    async fn status(&self, id: Uuid) -> String {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT status FROM workspace_invitation WHERE id=$1",
                &[&id],
            )
            .await
            .unwrap()
            .get(0)
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_accept_is_atomic_idempotent_and_scoped() {
    let f = Fixture::new(false).await;
    let token = jwt(f.invitee, 3600);
    let id = f.invitation(f.invitee, "1 day").await;
    let path = format!("/api/invitations/{id}/accept");
    for (caller, expected) in [
        (jwt(f.outsider, 3600), StatusCode::FORBIDDEN),
        (jwt(f.invitee, -300), StatusCode::UNAUTHORIZED),
    ] {
        assert_eq!(call(&f.app, "POST", &path, &caller).await.0, expected);
    }
    assert_eq!(
        call(&f.app, "POST", "/api/invitations/invalid/accept", &token)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(&path)
        .header("cookie", format!("multica_auth={token}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.status(id).await, "pending");
    let (a, b) = tokio::join!(
        call(&f.app, "POST", &path, &token),
        call(&f.app, "POST", &path, &token)
    );
    assert_eq!(a.0, StatusCode::OK, "{}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{}", b.1);
    assert_eq!(a.1["id"], b.1["id"]);
    assert_eq!(a.1["user_id"], f.invitee.to_string());
    assert_eq!(a.1["role"], "member");
    assert_eq!(f.status(id).await, "accepted");
    let db = f.pool.get().await.unwrap();
    let membership: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&f.ws, &f.invitee],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(membership, 1);
    let onboarded: bool = db
        .query_one(
            "SELECT onboarded_at IS NOT NULL FROM \"user\" WHERE id=$1",
            &[&f.invitee],
        )
        .await
        .unwrap()
        .get(0);
    assert!(onboarded);
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&f.ws, &f.invitee],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&f.app, "POST", &path, &token).await.0,
        StatusCode::BAD_REQUEST
    );
    let gone = f.invitation(f.outsider, "-1 day").await;
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{gone}/accept"),
            &jwt(f.outsider, 3600)
        )
        .await
        .0,
        StatusCode::GONE
    );
    assert_eq!(f.status(gone).await, "pending");
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn accepting_invitations_to_two_workspaces_does_not_deadlock_on_invitee() {
    let f = Fixture::new(false).await;
    let db = f.pool.get().await.unwrap();
    let other_workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other invitation workspace',$1) RETURNING id",
            &[&format!("other-invitations-{}", Uuid::new_v4().simple())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&other_workspace, &f.owner],
    )
    .await
    .unwrap();
    let first = f.invitation(f.invitee, "1 day").await;
    let email: String = db
        .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&f.invitee])
        .await
        .unwrap()
        .get(0);
    let second: Uuid = db
        .query_one(
            "INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES($1,$2,$3,$4,'member',now()+interval '1 day') RETURNING id",
            &[&other_workspace, &f.owner, &email, &f.invitee],
        )
        .await
        .unwrap()
        .get(0);
    drop(db);

    let token = jwt(f.invitee, 3600);
    let first_path = format!("/api/invitations/{first}/accept");
    let second_path = format!("/api/invitations/{second}/accept");
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::join!(
            call(&f.app, "POST", &first_path, &token),
            call(&f.app, "POST", &second_path, &token)
        )
    })
    .await
    .expect("parallel accepts stalled on invitee row locks");
    assert_eq!(a.0, StatusCode::OK, "{}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{}", b.1);
    assert_eq!(f.status(first).await, "accepted");
    assert_eq!(f.status(second).await, "accepted");
    let db = f.pool.get().await.unwrap();
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE user_id=$1 AND workspace_id IN ($2,$3)",
            &[&f.invitee, &f.ws, &other_workspace],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_uuid_request_forms_preserve_authorization_and_effects() {
    let f = Fixture::new(false).await;
    let token = jwt(f.invitee, 3600);
    let id = f.invitation(f.invitee, "1 day").await;
    let simple = id.simple();
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{simple}/decline"),
            &jwt(f.outsider, 3600)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.status(id).await, "pending");
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{simple}/decline"),
            &token
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(f.status(id).await, "declined");

    let revoke_id = f.invitation(f.invitee, "1 day").await;
    let workspace = f.ws.simple();
    let arbitrary = revoke_id.to_string().replace('-', "x");
    let revoke = format!("/api/workspaces/{workspace}/invitations/{arbitrary}");
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.member, 3600))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.status(revoke_id).await, "pending");
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.owner, 3600)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.owner, 3600)).await.0,
        StatusCode::NOT_FOUND
    );
    let invalid = format!("/api/invitations/{{{id}}}/accept");
    assert_eq!(
        call(&f.app, "POST", &invalid, &token).await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_decline_and_revoke_enforce_authority() {
    let f = Fixture::new(false).await;
    let id = f.invitation(f.invitee, "1 day").await;
    let decline = format!("/api/invitations/{id}/decline");
    assert_eq!(
        call(&f.app, "POST", &decline, &jwt(f.outsider, 3600))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&f.app, "POST", &decline, &jwt(f.invitee, 3600))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(f.status(id).await, "declined");
    assert_eq!(
        call(&f.app, "POST", &decline, &jwt(f.invitee, 3600))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let id = f.invitation(f.invitee, "1 day").await;
    let revoke = format!("/api/workspaces/{}/invitations/{id}", f.ws);
    let foreign: Uuid = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign',$1) RETURNING id",
            &[&format!("foreign-{}", Uuid::new_v4().simple())],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.outsider, 3600))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.member, 3600))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{foreign}/invitations/{id}"),
            &jwt(f.owner, 3600)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/invitations/invalid", f.ws),
            &jwt(f.admin, 3600)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.admin, 3600)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.admin, 3600)).await.0,
        StatusCode::NOT_FOUND
    );
    let left: i64 = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT count(*) FROM workspace_invitation WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_cloud_fails_closed_and_member_conflicts_roll_back() {
    let f = Fixture::new(true).await;
    let id = f.invitation(f.invitee, "1 day").await;
    let accept = format!("/api/invitations/{id}/accept");
    let decline = format!("/api/invitations/{id}/decline");
    let revoke = format!("/api/workspaces/{}/invitations/{id}", f.ws);
    assert_eq!(
        call(&f.app, "POST", &accept, &jwt(f.invitee, 3600)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&f.app, "POST", &decline, &jwt(f.invitee, 3600))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&f.app, "DELETE", &revoke, &jwt(f.owner, 3600)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.status(id).await, "pending");
    let bare = app(f.pool.clone(), false);
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&f.ws, &f.invitee],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&bare, "POST", &accept, &jwt(f.invitee, 3600)).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(f.status(id).await, "pending");
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_task_credential_cannot_revoke_or_cross_workspace() {
    let f = Fixture::new(false).await;
    let db = f.pool.get().await.unwrap();
    let agent: Uuid = db
        .query_one(
            "INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Invitation test agent','cloud',$2) RETURNING id",
            &[&f.ws, &f.owner],
        )
        .await
        .unwrap()
        .get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Invitation test runtime','cloud','synthetic','online') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_invitation_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    let id = f.invitation(f.invitee, "1 day").await;
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/invitations/{id}", f.ws),
            &token
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        invite(
            &f.app,
            f.ws,
            &token,
            json!({"email":"task-cannot-invite@example.com"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let own = f.invitation(f.owner, "1 day").await;
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other invitation',$1) RETURNING id",
            &[&format!("other-invitation-{}", Uuid::new_v4().simple())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE workspace_invitation SET workspace_id=$2 WHERE id=$1",
        &[&own, &foreign],
    )
    .await
    .unwrap();
    for action in ["accept", "decline"] {
        assert_eq!(
            call(
                &f.app,
                "POST",
                &format!("/api/invitations/{own}/{action}"),
                &token
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }
    db.execute(
        "UPDATE workspace_invitation SET workspace_id=$2 WHERE id=$1",
        &[&own, &f.ws],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{own}/accept"),
            &token
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/invitations/{id}", f.ws),
            &token
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(f.status(id).await, "pending");
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_onboarding_fault_rolls_back_and_settlement_races_are_consistent() {
    let f = Fixture::new(false).await;
    let db = f.pool.get().await.unwrap();
    let id = f.invitation(f.invitee, "1 day").await;
    let path = format!("/api/invitations/{id}/accept");
    let token = jwt(f.invitee, 3600);
    db.batch_execute(&format!(
        "CREATE FUNCTION pg_temp.reject_invitation_onboarding() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{}'::uuid THEN RAISE EXCEPTION 'synthetic onboarding fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER account_catalog_invitation_fault BEFORE UPDATE OF onboarded_at ON \"user\" FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_invitation_onboarding()", f.invitee
    )).await.unwrap();
    let failed = call(&f.app, "POST", &path, &token).await;
    db.batch_execute("DROP TRIGGER account_catalog_invitation_fault ON \"user\"")
        .await
        .unwrap();
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{}", failed.1);
    assert_eq!(failed.1["error"], "failed to mark user onboarded");
    assert_eq!(f.status(id).await, "pending");
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&f.ws, &f.invitee],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let decline_path = format!("/api/invitations/{id}/decline");
    let (accepted, declined) = tokio::join!(
        call(&f.app, "POST", &path, &token),
        call(&f.app, "POST", &decline_path, &token)
    );
    assert_eq!(declined.0, StatusCode::NO_CONTENT);
    let status = f.status(id).await;
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&f.ws, &f.invitee],
        )
        .await
        .unwrap()
        .get(0);
    match status.as_str() {
        "accepted" => {
            assert_eq!(accepted.0, StatusCode::OK);
            assert_eq!(count, 1);
        }
        "declined" => {
            assert_eq!(accepted.0, StatusCode::BAD_REQUEST);
            assert_eq!(count, 0);
        }
        other => panic!("unexpected final status {other}"),
    }
}

async fn invite_response(
    app: &axum::Router,
    ws: Uuid,
    token: &str,
    body: Value,
) -> axum::response::Response {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/workspaces/{ws}/members"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn invite_raw(
    app: &axum::Router,
    ws: Uuid,
    token: &str,
    body: String,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/workspaces/{ws}/members"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn invite(app: &axum::Router, ws: Uuid, token: &str, body: Value) -> (StatusCode, Value) {
    let response = invite_response(app, ws, token, body).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_create_validates_roles_and_human_recipient() {
    let f = Fixture::new(false).await;
    let db = f.pool.get().await.unwrap();
    let email: String = db
        .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&f.invitee])
        .await
        .unwrap()
        .get(0);
    let owner = jwt(f.owner, 3600);
    let body = json!({"email": email.to_uppercase(), "role": "admin"});
    for (caller, expected) in [
        (jwt(f.member, 3600), StatusCode::FORBIDDEN),
        (jwt(f.outsider, 3600), StatusCode::NOT_FOUND),
        (jwt(f.owner, -300), StatusCode::UNAUTHORIZED),
    ] {
        assert_eq!(
            invite(&f.app, f.ws, &caller, body.clone()).await.0,
            expected
        );
    }
    assert_eq!(
        invite(&f.app, f.ws, &owner, json!({"email": ""})).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        invite(
            &f.app,
            f.ws,
            &owner,
            json!({"email": email, "role":"owner"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        invite(
            &f.app,
            f.ws,
            &owner,
            json!({"email": "shadow@channel.invalid"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&f.invitee],
    )
    .await
    .unwrap();
    assert_eq!(
        invite(&f.app, f.ws, &owner, body.clone()).await.0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=false WHERE id=$1",
        &[&f.invitee],
    )
    .await
    .unwrap();
    let (status, result) = invite(&f.app, f.ws, &owner, body.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    assert_eq!(result["invitee_email"], email);
    assert_eq!(result["invitee_user_id"], f.invitee.to_string());
    assert_eq!(result["role"], "admin");
    assert_eq!(result["status"], "pending");
    assert_eq!(result["workspace_id"], f.ws.to_string());
    assert!(result["expires_at"].is_string());
    assert_eq!(
        invite(&f.app, f.ws, &owner, body).await.0,
        StatusCode::CONFLICT
    );
    let owner_email: String = db
        .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&f.owner])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        invite(&f.app, f.ws, &owner, json!({"email": owner_email}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    for invalid in [
        Value::Null,
        json!({"email": 42}),
        json!({"email":email,"role":42}),
        json!({"email":email,"role":" "}),
    ] {
        assert_eq!(
            invite(&f.app, f.ws, &owner, invalid).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let count: i64 = db.query_one("SELECT count(*) FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email=$2 AND status='pending'", &[&f.ws,&email]).await.unwrap().get(0);
    assert_eq!(count, 1);
    // Go decodes the first JSON value without a Content-Type header and folds field names.
    let raw_email = format!("raw-{}@invalid.example", Uuid::new_v4().simple());
    let (status, created) = invite_raw(
        &f.app,
        f.ws,
        &owner,
        format!(r#"{{"EMAIL":"{raw_email}","email":null,"Role":"admin","role":null}} trailing"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["invitee_email"], raw_email);
    assert_eq!(created["role"], "admin");
    let huge_email = format!("huge-{}@invalid.example", Uuid::new_v4().simple());
    let huge = format!(r#"{{"email":"{huge_email}"}}{}"#, " ".repeat(2_200_000));
    assert_eq!(
        invite_raw(&f.app, f.ws, &owner, huge).await.0,
        StatusCode::CREATED
    );
    let dropped = format!("drop-{}@invalid.example", Uuid::new_v4().simple());
    assert_eq!(
        invite_raw(
            &f.app,
            f.ws,
            &owner,
            format!(r#"{{"email":"{dropped}","EMAIL":42}}"#),
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let dropped_count: i64 = db
        .query_one(
            "SELECT count(*) FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email=$2",
            &[&f.ws, &dropped],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(dropped_count, 0);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_email_uses_go_single_rune_lowercase_on_create_and_accept() {
    let f = Fixture::new(false).await;
    let db = f.pool.get().await.unwrap();
    let email = format!("İ-{}@invalid.example", Uuid::new_v4().simple());
    db.execute(
        "UPDATE \"user\" SET email=$1 WHERE id=$2",
        &[&email, &f.invitee],
    )
    .await
    .unwrap();
    let expected = email.replacen('İ', "i", 1);
    let (status, created) =
        invite(&f.app, f.ws, &jwt(f.owner, 3600), json!({"email": email})).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["invitee_email"], expected);
    assert!(created["invitee_user_id"].is_null());
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let stored: String = db
        .query_one(
            "SELECT invitee_email FROM workspace_invitation WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, expected);
    let (status, member) = call(
        &f.app,
        "POST",
        &format!("/api/invitations/{id}/accept"),
        &jwt(f.invitee, 3600),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{member}");
    assert_eq!(member["user_id"], f.invitee.to_string());
    assert_eq!(f.status(id).await, "accepted");
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_create_expiry_and_concurrency_are_atomic() {
    let f = Fixture::new(false).await;
    let token = jwt(f.admin, 3600);
    let db = f.pool.get().await.unwrap();
    let stale: Uuid = db.query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role,expires_at) VALUES($1,$2,'fresh@example.com','member',now()-interval '1 day') RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let body = json!({"email":"  FRESH@example.com "});
    let (first, second) = tokio::join!(
        invite(&f.app, f.ws, &token, body.clone()),
        invite(&f.app, f.ws, &token, body.clone())
    );
    assert!(
        matches!(
            (first.0, second.0),
            (StatusCode::CREATED, StatusCode::CONFLICT)
                | (StatusCode::CONFLICT, StatusCode::CREATED)
        ),
        "{first:?} {second:?}"
    );
    assert_eq!(f.status(stale).await, "expired");
    let count: i64 = db.query_one("SELECT count(*) FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email='fresh@example.com' AND status='pending'", &[&f.ws]).await.unwrap().get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_create_cloud_does_not_write_and_rejects_cookie_without_csrf() {
    let f = Fixture::new(true).await;
    let token = jwt(f.owner, 3600);
    let body = json!({"email":"cloud-test@example.com"});
    assert_eq!(
        invite(&f.app, f.ws, &token, body.clone()).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(format!("/api/workspaces/{}/members", f.ws))
        .header("cookie", format!("multica_auth={token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let count: i64 = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT count(*) FROM workspace_invitation WHERE workspace_id=$1",
            &[&f.ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn invitation_create_rate_limits_actor_without_mutating_rejected_invites() {
    let f = Fixture::new(false).await;
    let token = jwt(f.owner, 3600);
    for n in 0..10 {
        let (status, value) = invite(
            &f.app,
            f.ws,
            &token,
            json!({"email":format!("rate-{n}@example.com")}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{value}");
    }
    let response = invite_response(
        &f.app,
        f.ws,
        &token,
        json!({"email":"rate-blocked@example.com"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let retry: u64 = response.headers()["retry-after"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["code"], "invitation_rate_limited");
    assert_eq!(body["retry_after_seconds"], retry);
    assert_eq!(
        invite(
            &f.app,
            f.ws,
            &jwt(f.admin, 3600),
            json!({"email":"rate-blocked@example.com"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let count: i64 = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT count(*) FROM workspace_invitation WHERE workspace_id=$1",
            &[&f.ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 11);
}
