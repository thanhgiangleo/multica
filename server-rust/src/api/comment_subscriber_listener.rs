//! Go `registerSubscriberListeners`, comment arm: a committed `comment:created`
//! auto-subscribes its author to the issue with reason `commenter`.
//!
//! Go runs this synchronously inside `bus.Publish`, so the subscriber row
//! exists before the publishing handler returns. The Rust bus is a broadcast
//! channel, so the effect is applied by a consumer task instead; the contract
//! is the same one Go documents — best-effort, logged, never fatal, because the
//! comment is already committed and a subscription hiccup must not look like a
//! failed write.
//!
//! Only the comment arm is ported. The `issue:created` and `issue:updated`
//! arms depend on producers that are not wired on this binary
//! (`issue_create_runtime`), so porting them would add unreachable code.
use super::{
    ApiState,
    runtime_chat_events::{AllEvents, Event, Events},
};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

/// Byte-identical to Go's generated `addIssueSubscriber` const.
const ADD_SUBSCRIBER_SQL: &str = include_str!("add_issue_subscriber.sql");

/// Start the single in-process listener. Go registers subscriber listeners
/// before the notification listeners and before serving traffic.
pub fn spawn(state: &ApiState, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
    let events = state.events.clone();
    let pool = state.pool.clone();
    // Subscribe on the caller's thread: a broadcast receiver that only exists
    // once the spawned task first polls would miss a comment committed in that
    // window, and Go has no such window because Publish runs the listener
    // inline.
    let bus = events.subscribe_all();
    tokio::spawn(run(events, pool, bus, shutdown))
}

async fn run(
    events: Events,
    pool: Arc<Pool>,
    mut bus: AllEvents,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            received = bus.recv() => match received {
                Ok(event) => handle(&events, &pool, event).await,
                // Go's bus never drops an event. A broadcast receiver can, and
                // there is nothing to replay from, so say so loudly instead of
                // pretending the missed comments were handled.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!(
                        "comment subscriber listener: lagged, skipped {skipped} events; \
                         affected comments need subscriber rows refetched"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
        }
        if *shutdown.borrow() {
            break;
        }
    }
}

/// The gate Go applies before `addSubscriber` in the comment arm.
///
/// Returns the three payload strings exactly as the producer wrote them, since
/// those are what Go echoes back in `subscriber:added`.
fn comment_target(payload: &Value) -> Option<(&str, &str, &str)> {
    // Handler-created comments publish `handler.CommentResponse` and agent
    // comments publish `map[string]any`; both reach this listener as one JSON
    // object. A payload that is not an object, or has no comment in it, is not
    // this listener's event.
    let comment = payload.get("comment").and_then(Value::as_object)?;
    let issue_id = comment
        .get("issue_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let author_type = comment
        .get("author_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let author_id = comment
        .get("author_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if issue_id.is_empty() || author_id.is_empty() {
        return None;
    }

    // Platform-authored system comments (MUL-2538 child-done parent notify)
    // have author_type='system' and a zero UUID author. They must NOT add a
    // subscriber row: issue_subscriber.user_type is constrained to
    // ('member','agent'), and a "system" subscriber has no inbox to read
    // anyway. Skip them at the side-effect boundary so the system event stays a
    // pure WS broadcast for the timeline.
    if author_type == "system" {
        return None;
    }
    Some((issue_id, author_type, author_id))
}

async fn handle(events: &Events, pool: &Pool, event: Event) {
    let Some((issue_id, author_type, author_id)) = comment_target(&event.payload) else {
        return;
    };

    // Go's parseUUID is util.MustParseUUID and panics on a malformed id. A
    // side effect must not take the process down, so an unparseable pair is
    // logged and skipped; the payload strings are what Go broadcasts back out.
    let (Ok(issue_uuid), Ok(author_uuid)) = (Uuid::parse_str(issue_id), Uuid::parse_str(author_id))
    else {
        eprintln!(
            "failed to add issue subscriber: invalid uuid issue_id={issue_id} user_id={author_id}"
        );
        return;
    };

    let client = match pool.get().await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to add issue subscriber: pool acquire failed: {err}");
            return;
        }
    };
    let affected = match client
        .execute(
            ADD_SUBSCRIBER_SQL,
            &[&issue_uuid, &author_type, &author_uuid, &"commenter"],
        )
        .await
    {
        Ok(affected) => affected,
        Err(err) => {
            eprintln!(
                "failed to add issue subscriber: issue_id={issue_id} user_type={author_type} \
                 user_id={author_id} reason=commenter error={err}"
            );
            return;
        }
    };

    // Nothing changed — either an identical active row already existed, or the
    // insert hit an opt-out tombstone and was correctly refused. Broadcasting
    // `subscriber:added` anyway made every open client insert a subscriber the
    // DB does not have: the UI claimed the user was watching while they stayed
    // unsubscribed and kept receiving nothing.
    if affected == 0 {
        return;
    }

    events.publish(Event {
        event_type: "subscriber:added".into(),
        workspace_id: event.workspace_id,
        // Go publishes this event with no actor fields, which the wire format
        // renders as an empty actor type and a nil actor id.
        actor_type: String::new(),
        actor_id: Uuid::nil(),
        chat_session_id: None,
        chat_session_owner_id: None,
        payload: json!({
            "issue_id": issue_id,
            "user_type": author_type,
            "user_id": author_id,
            "reason": "commenter",
        }),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISSUE: &str = "01a1923b-6cac-7cc3-9079-09b80ba9840d";
    const USER: &str = "01a1a6c1-92f7-70b8-8f6c-0d3d4a3f6444";

    fn payload(issue: &str, author_type: &str, author: &str) -> Value {
        json!({"comment": {
            "issue_id": issue,
            "author_type": author_type,
            "author_id": author,
        }})
    }

    #[test]
    fn comment_arm_matches_go_gate() {
        assert_eq!(
            comment_target(&payload(ISSUE, "member", USER)),
            Some((ISSUE, "member", USER))
        );
        assert_eq!(
            comment_target(&payload(ISSUE, "agent", USER)),
            Some((ISSUE, "agent", USER))
        );
        // MUL-2538: a system comment never adds a subscriber row.
        assert_eq!(comment_target(&payload(ISSUE, "system", USER)), None);
        // `if issueID == "" || authorID == "" { return }`
        assert_eq!(comment_target(&payload("", "member", USER)), None);
        assert_eq!(comment_target(&payload(ISSUE, "member", "")), None);
    }

    #[test]
    fn non_comment_payloads_are_ignored() {
        assert_eq!(comment_target(&json!({})), None);
        assert_eq!(comment_target(&json!({"comment": null})), None);
        assert_eq!(comment_target(&json!({"comment": "text"})), None);
        assert_eq!(comment_target(&json!({"issue": {"id": ISSUE}})), None);
        // A comment without the author fields is not a subscriber payload.
        assert_eq!(comment_target(&json!({"comment": {"id": ISSUE}})), None);
        // Non-string ids are not read, matching Go's type assertions.
        assert_eq!(
            comment_target(&json!({"comment": {"issue_id": 1, "author_id": 2}})),
            None
        );
    }

    #[test]
    fn add_subscriber_sql_is_byte_identical_to_go_oracle() {
        assert_eq!(
            ADD_SUBSCRIBER_SQL,
            include_str!("../../tests/add_issue_subscriber_frozen_go.sql"),
            "add_issue_subscriber.sql drifted from Go's addIssueSubscriber const"
        );
    }

    /// The row the comment arm owns for one (issue, member) pair, or `None`
    /// when the comment never inserted one.
    async fn subscriber_row(
        db: &tokio_postgres::Client,
        issue: Uuid,
        author: Uuid,
    ) -> Option<tokio_postgres::Row> {
        db.query_opt(
            "SELECT reason, unsubscribed_at IS NULL AS active \
             FROM issue_subscriber \
             WHERE issue_id=$1 AND user_type='member' AND user_id=$2",
            &[&issue, &author],
        )
        .await
        .unwrap()
    }

    /// End-to-end on a disposable migrated database: a committed
    /// `comment:created` writes the subscriber row and fans `subscriber:added`
    /// out to the workspace audience, while a system comment and an opt-out
    /// tombstone both stay inert.
    #[tokio::test]
    #[ignore = "requires child-owned migrated multica_zbsi60 on an isolated loopback port"]
    async fn comment_created_subscribes_author_and_broadcasts_to_workspace() {
        use std::{str::FromStr, time::Duration};
        use tokio_postgres::{Config, NoTls};

        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi60"));
        assert!(config.get_hosts().iter().all(|host| matches!(
            host, tokio_postgres::config::Host::Tcp(value) if value == "127.0.0.1"
        )));
        assert!(config.get_ports().iter().all(|port| *port != 5432));
        let (db, connection) = config.connect(NoTls).await.unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });

        let suffix = Uuid::new_v4().simple().to_string();
        let author: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('Commenter',$1) RETURNING id",
                &[&format!("commenter-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let workspace: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES ('WS',$1) RETURNING id",
                &[&format!("ws-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        let other_workspace: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES ('Other WS',$1) RETURNING id",
                &[&format!("ws-other-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        let issue: Uuid = db
            .query_one(
                "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) \
                 VALUES ($1,'Comment issue','member',$2,1) RETURNING id",
                &[&workspace, &author],
            )
            .await
            .unwrap()
            .get(0);

        let state = ApiState::new(
            Arc::new(
                deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
                    config.clone(),
                    NoTls,
                ))
                .max_size(2)
                .build()
                .unwrap(),
            ),
            b"disposable-test-secret".to_vec(),
        );
        let (stop, shutdown) = watch::channel(false);
        let listener = spawn(&state, shutdown);
        let mut member = state.events.subscribe_for(workspace, author);
        let mut stranger = state.events.subscribe_for(other_workspace, author);

        let comment = |author_type: &str, author_id: Uuid| {
            json!({
                "comment": {
                    "id": Uuid::new_v4().to_string(),
                    "issue_id": issue.to_string(),
                    "author_type": author_type,
                    "author_id": author_id.to_string(),
                },
                "issue_title": "Comment issue",
                "issue_revision": 1,
            })
        };
        let publish = |payload: Value, actor_type: &str, actor: Uuid| {
            state.events.publish(Event {
                event_type: "comment:created".into(),
                workspace_id: workspace,
                actor_type: actor_type.into(),
                actor_id: actor,
                chat_session_id: None,
                chat_session_owner_id: None,
                payload,
            });
        };

        // A member comment subscribes its author and tells the workspace.
        publish(comment("member", author), "member", author);
        let added = next_of_type(&mut member, "subscriber:added", Duration::from_secs(10))
            .await
            .expect("subscriber:added must reach the workspace audience");
        assert_eq!(added.event_type, "subscriber:added");
        assert_eq!(added.workspace_id, workspace);
        // Go publishes this event with no actor fields.
        assert_eq!(added.actor_type, "");
        assert_eq!(added.actor_id, Uuid::nil());
        assert_eq!(
            added.payload,
            json!({
                "issue_id": issue.to_string(),
                "user_type": "member",
                "user_id": author.to_string(),
                "reason": "commenter",
            })
        );
        let row = subscriber_row(&db, issue, author)
            .await
            .expect("the comment must insert an issue_subscriber row");
        assert_eq!(row.get::<_, String>("reason"), "commenter");
        assert!(row.get::<_, bool>("active"));

        // Go's bus is workspace-scoped for sockets: another workspace sees
        // nothing, even for the same user id.
        assert!(
            next_of_type(&mut stranger, "subscriber:added", Duration::from_secs(1))
                .await
                .is_none(),
            "subscriber:added must not cross the workspace boundary"
        );

        // MUL-2538: a system comment never adds a subscriber row.
        publish(comment("system", Uuid::nil()), "system", Uuid::nil());
        assert!(
            next_of_type(&mut member, "subscriber:added", Duration::from_secs(1))
                .await
                .is_none(),
            "a system comment must not broadcast subscriber:added"
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*)::bigint FROM issue_subscriber WHERE issue_id=$1",
                &[&issue]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );

        // An opt-out tombstone is not resurrected, and nothing is broadcast
        // for a write that changed no row.
        db.execute(
            "UPDATE issue_subscriber SET unsubscribed_at=now(), opt_out_scope='issue' \
             WHERE issue_id=$1 AND user_type='member' AND user_id=$2",
            &[&issue, &author],
        )
        .await
        .unwrap();
        publish(comment("member", author), "member", author);
        assert!(
            next_of_type(&mut member, "subscriber:added", Duration::from_secs(1))
                .await
                .is_none(),
            "a tombstoned subscriber must not be re-added or announced"
        );
        let row = subscriber_row(&db, issue, author)
            .await
            .expect("the tombstone row must survive the repeat comment");
        assert!(!row.get::<_, bool>("active"));
        assert_eq!(row.get::<_, String>("reason"), "commenter");

        let _ = stop.send(true);
        listener.await.unwrap();
        db.execute("DELETE FROM issue WHERE workspace_id=$1", &[&workspace])
            .await
            .unwrap();
        for workspace in [workspace, other_workspace] {
            db.execute("DELETE FROM workspace WHERE id=$1", &[&workspace])
                .await
                .unwrap();
        }
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&author])
            .await
            .unwrap();
        connection.abort();
    }

    /// Skip unrelated traffic on the shared bus until the wanted event or the
    /// deadline. A timeout is the assertion "this fanout did not happen".
    async fn next_of_type(
        events: &mut super::super::runtime_chat_events::ScopedEvents,
        kind: &str,
        deadline: std::time::Duration,
    ) -> Option<Event> {
        let until = tokio::time::Instant::now() + deadline;
        loop {
            match tokio::time::timeout_at(until, events.recv()).await {
                Ok(Ok(event)) if event.event_type == kind => return Some(event),
                Ok(Ok(_)) => continue,
                Ok(Err(error)) => panic!("subscription failed: {error}"),
                Err(_) => return None,
            }
        }
    }
}
