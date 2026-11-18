use super::*;
use multica_server::api::channels_sprint_notify_event::{
    Assignee, ChangeField, TrustedEvent, TrustedEventKind, apply_trusted_event,
};
use std::collections::HashSet;

async fn inbox_count(db: &deadpool_postgres::Object, issue: Uuid) -> i64 {
    db.query_one(
        "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
        &[&issue],
    )
    .await
    .unwrap()
    .get(0)
}
async fn inbox_kinds(
    db: &deadpool_postgres::Object,
    rows: &[multica_server::api::channels_sprint_notify::InboxDelivery],
) -> HashSet<(Uuid, String)> {
    let ids: Vec<Uuid> = rows.iter().map(|r| r.id).collect();
    db.query(
        "SELECT recipient_id,type FROM inbox_item WHERE id=ANY($1)",
        &[&ids],
    )
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn notification_event_mapping_is_atomic_and_system_comments_are_inert() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "event-owner", &suffix).await;
    let new = insert_user(&db, "event-new", &suffix).await;
    let old = insert_user(&db, "event-old", &suffix).await;
    let watcher = insert_user(&db, "event-watcher", &suffix).await;
    let muted = insert_user(&db, "event-muted", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Event targets',$1) RETURNING id",
            &[&format!("events-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign events',$1) RETURNING id",
            &[&format!("events-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for u in [owner, new, old, watcher, muted] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
            &[&workspace, &u],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Event agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES($1,'Event squad',$2,$3) RETURNING id", &[&workspace,&agent,&owner]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id,assignee_type,assignee_id) VALUES($1,1,'Canonical DB title','member',$2,'member',$3) RETURNING id", &[&workspace,&owner,&new]).await.unwrap().get(0);
    for u in [owner, new, old, watcher] {
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&issue,&u]).await.unwrap();
    }
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES($1,$2,$3)",
        &[
            &workspace,
            &muted,
            &json!({"comments":"muted","mentions":"muted"}),
        ],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES($1,$2,$3)",
        &[&workspace, &new, &json!({"comments":"muted"})],
    )
    .await
    .unwrap();
    let mut event = TrustedEvent {
        id: Uuid::new_v4(),
        workspace,
        issue,
        actor: Some(owner),
        actor_type: "member".into(),
        kind: TrustedEventKind::IssueCreated {
            mentions: vec![
                ParsedMention::Member(new),
                ParsedMention::Member(old),
                ParsedMention::Member(muted),
            ],
        },
    };
    let created = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(
        inbox_kinds(&db, &created).await,
        [(new, "issue_assigned".into()), (old, "mentioned".into())]
            .into_iter()
            .collect()
    );
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.id = Uuid::new_v4();
    db.execute(
        "UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1",
        &[&issue, &agent],
    )
    .await
    .unwrap();
    event.kind = TrustedEventKind::AssigneeChanged {
        previous: Some(Assignee::Member(old)),
    };
    let assigned = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(
        inbox_kinds(&db, &assigned).await,
        [
            (agent, "issue_assigned".into()),
            (old, "unassigned".into()),
            (new, "assignee_changed".into()),
            (watcher, "assignee_changed".into())
        ]
        .into_iter()
        .collect()
    );
    assert!(
        assigned
            .iter()
            .any(|r| r.recipient == agent && r.recipient_type == "agent")
    );
    let details: Value = db
        .query_one(
            "SELECT details FROM inbox_item WHERE id=$1",
            &[&assigned[0].id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(details["new_assignee_type"], json!("agent"));
    assert_eq!(details["prev_assignee_id"], json!(old.to_string()));
    assert!(details.as_object().unwrap().values().all(Value::is_string));
    db.execute(
        "UPDATE issue SET assignee_type='member',assignee_id=$2 WHERE id=$1",
        &[&issue, &new],
    )
    .await
    .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::AssigneeChanged {
        previous: Some(Assignee::Agent(agent)),
    };
    let from_agent = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(
        inbox_kinds(&db, &from_agent).await,
        [
            (new, "issue_assigned".into()),
            (old, "assignee_changed".into()),
            (watcher, "assignee_changed".into())
        ]
        .into_iter()
        .collect()
    );
    db.execute(
        "UPDATE issue SET assignee_type='squad',assignee_id=$2 WHERE id=$1",
        &[&issue, &squad],
    )
    .await
    .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::AssigneeChanged {
        previous: Some(Assignee::Member(new)),
    };
    let to_squad = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert!(
        to_squad
            .iter()
            .all(|r| r.recipient_type == "member" && r.recipient != squad)
    );
    let details: Value = db
        .query_one(
            "SELECT details FROM inbox_item WHERE id=$1",
            &[&to_squad[0].id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(details["new_assignee_type"], json!("squad"));
    assert_eq!(details["new_assignee_id"], json!(squad.to_string()));
    let comment: Uuid = db.query_one("INSERT INTO comment(issue_id,author_type,author_id,content,workspace_id) VALUES($1,'member',$2,'Canonical DB comment',$3) RETURNING id", &[&issue,&owner,&workspace]).await.unwrap().get(0);
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::CommentCreated {
        comment,
        mentions: vec![
            ParsedMention::Member(new),
            ParsedMention::Member(old),
            ParsedMention::Member(muted),
        ],
    };
    let commented = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(
        inbox_kinds(&db, &commented).await,
        [
            (old, "new_comment".into()),
            (watcher, "new_comment".into()),
            (new, "mentioned".into()),
            (old, "mentioned".into())
        ]
        .into_iter()
        .collect()
    );
    for d in &commented {
        let row = db
            .query_one(
                "SELECT title,body,type,details FROM inbox_item WHERE id=$1",
                &[&d.id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>("title"), "Canonical DB title");
        assert_eq!(
            row.get::<_, Value>("details")["comment_id"],
            json!(comment.to_string())
        );
        assert_eq!(
            row.get::<_, Option<String>>("body").as_deref(),
            Some(if row.get::<_, String>("type") == "new_comment" {
                "Canonical DB comment"
            } else {
                ""
            })
        );
    }
    // Platform comments may transclude malicious-looking mention text. Their
    // stored author type wins even if the caller supplied expanded targets.
    let system: Uuid = db.query_one("INSERT INTO comment(issue_id,author_type,author_id,content,type,workspace_id) VALUES($1,'system',$2,'[@all](mention://all/all)','system',$3) RETURNING id", &[&issue,&Uuid::nil(),&workspace]).await.unwrap().get(0);
    let before = inbox_count(&db, issue).await;
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::CommentCreated {
        comment: system,
        mentions: vec![ParsedMention::All],
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    assert_eq!(inbox_count(&db, issue).await, before);
    event.kind = TrustedEventKind::CommentCreated {
        comment: Uuid::new_v4(),
        mentions: vec![ParsedMention::All],
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.workspace = foreign;
    event.kind = TrustedEventKind::IssueCreated {
        mentions: vec![ParsedMention::All],
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.workspace = workspace;
    // Failure only during mentions must undo subscriber rows created earlier.
    db.batch_execute("CREATE FUNCTION channels_sprint_event_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='mentioned' THEN RAISE EXCEPTION 'synthetic event rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_event_abort BEFORE INSERT ON inbox_item FOR EACH ROW EXECUTE FUNCTION channels_sprint_event_abort();").await.unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::CommentCreated {
        comment,
        mentions: vec![ParsedMention::Member(new)],
    };
    assert!(apply_trusted_event(&pool, &event).await.is_err());
    assert_eq!(inbox_count(&db, issue).await, before);
    db.batch_execute("DROP TRIGGER channels_sprint_event_abort ON inbox_item; DROP FUNCTION channels_sprint_event_abort();").await.unwrap();
    assert_eq!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .len(),
        3
    );
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::DescriptionMentionsAdded {
        mentions: vec![ParsedMention::Member(new), ParsedMention::Member(new)],
    };
    let changed = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(
        inbox_kinds(&db, &changed).await,
        [(new, "mentioned".into())].into_iter().collect()
    );
    for (field, kind) in [
        (ChangeField::Status, "status_changed"),
        (ChangeField::Priority, "priority_changed"),
        (ChangeField::StartDate, "start_date_changed"),
        (ChangeField::DueDate, "due_date_changed"),
    ] {
        event.id = Uuid::new_v4();
        event.kind = TrustedEventKind::FieldChanged {
            field,
            from: "old".into(),
            to: "new".into(),
        };
        let rows = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
        assert_eq!(
            inbox_kinds(&db, &rows).await,
            [
                (old, kind.into()),
                (new, kind.into()),
                (watcher, kind.into())
            ]
            .into_iter()
            .collect()
        );
    }
    // A status handoff retires prior failure notices only after the entire
    // status notification transaction succeeds. Blocked is not completion.
    let old_failures = [Uuid::new_v4(), Uuid::new_v4()];
    let watcher_failure = Uuid::new_v4();
    for (id, recipient) in [
        (old_failures[0], old),
        (old_failures[1], old),
        (watcher_failure, watcher),
    ] {
        db.execute("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title) VALUES($1,$2,'member',$3,'task_failed','action_required',$4,'Old failure')", &[&id,&workspace,&recipient,&issue]).await.unwrap();
    }
    let agent_failure = Uuid::new_v4();
    let foreign_failure = Uuid::new_v4();
    let archived_failure = Uuid::new_v4();
    db.execute("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title) VALUES($1,$2,'agent',$3,'task_failed','action_required',$4,'Agent failure')", &[&agent_failure,&workspace,&agent,&issue]).await.unwrap();
    db.execute("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title) VALUES($1,$2,'member',$3,'task_failed','action_required',$4,'Foreign workspace row')", &[&foreign_failure,&foreign,&old,&issue]).await.unwrap();
    db.execute("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title,archived) VALUES($1,$2,'member',$3,'task_failed','action_required',$4,'Already archived',true)", &[&archived_failure,&workspace,&old,&issue]).await.unwrap();
    db.execute(
        "UPDATE issue SET status='missing_event_status' WHERE id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::FieldChanged {
        field: ChangeField::Status,
        from: "todo".into(),
        to: "missing_event_status".into(),
    };
    let before = inbox_count(&db, issue).await;
    assert!(matches!(
        apply_trusted_event(&pool, &event).await,
        Err(multica_server::api::channels_sprint_notify::FanoutError::UnknownStatus)
    ));
    assert_eq!(inbox_count(&db, issue).await, before);
    db.execute("UPDATE issue SET status='blocked' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::FieldChanged {
        field: ChangeField::Status,
        from: "in_progress".into(),
        to: "blocked".into(),
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .archived
            .is_empty()
    );
    db.execute("UPDATE issue SET status='in_review' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::FieldChanged {
        field: ChangeField::Status,
        from: "blocked".into(),
        to: "in_review".into(),
    };
    db.batch_execute("CREATE FUNCTION channels_sprint_archive_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='task_failed' AND NEW.archived THEN RAISE EXCEPTION 'synthetic archive rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_archive_abort BEFORE UPDATE ON inbox_item FOR EACH ROW EXECUTE FUNCTION channels_sprint_archive_abort();").await.unwrap();
    let before = inbox_count(&db, issue).await;
    assert!(apply_trusted_event(&pool, &event).await.is_err());
    assert_eq!(inbox_count(&db, issue).await, before);
    let active_after_rollback: i64 = db.query_one("SELECT COUNT(*) FROM inbox_item WHERE workspace_id=$1 AND issue_id=$2 AND type='task_failed' AND NOT archived", &[&workspace,&issue]).await.unwrap().get(0);
    assert_eq!(active_after_rollback, 4);
    db.batch_execute("DROP TRIGGER channels_sprint_archive_abort ON inbox_item; DROP FUNCTION channels_sprint_archive_abort();").await.unwrap();
    let handoff = apply_trusted_event(&pool, &event).await.unwrap();
    assert_eq!(handoff.archived.len(), 2);
    assert!(handoff.archived.iter().any(|a| a.recipient == old
        && a.count == 2
        && a.workspace == workspace
        && a.issue == issue
        && a.reason == "issue_status_terminal"));
    assert!(
        handoff
            .archived
            .iter()
            .any(|a| a.recipient == watcher && a.count == 1)
    );
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .archived
            .is_empty()
    );
    let remaining: i64 = db.query_one("SELECT COUNT(*) FROM inbox_item WHERE workspace_id=$1 AND issue_id=$2 AND type='task_failed' AND NOT archived", &[&workspace,&issue]).await.unwrap().get(0);
    assert_eq!(remaining, 0);
    let foreign_archived: bool = db
        .query_one(
            "SELECT archived FROM inbox_item WHERE id=$1 AND workspace_id=$2",
            &[&foreign_failure, &foreign],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!foreign_archived);
    let agent_archived: bool = db
        .query_one(
            "SELECT archived FROM inbox_item WHERE id=$1",
            &[&agent_failure],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        agent_archived,
        "agent rows are archived but never published to member feeds"
    );
    let tasks: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM agent_task_queue WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        tasks, 0,
        "notifications must never dispatch agents or contact providers"
    );
}
