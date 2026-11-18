use super::*;
use multica_server::api::{
    channels_sprint_notify::FanoutError,
    channels_sprint_notify_event::{TrustedEvent, TrustedEventKind, apply_trusted_event},
};

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn task_failure_and_reactions_respect_workspace_identity_and_replay() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(cfg.get_hosts().iter().all(|h| matches!(
        h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"
    )));
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let creator = insert_user(&db, "reaction-creator", &suffix).await;
    let watcher = insert_user(&db, "reaction-watcher", &suffix).await;
    let muted = insert_user(&db, "reaction-muted", &suffix).await;
    let outsider = insert_user(&db, "reaction-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Reaction test',$1) RETURNING id",
            &[&format!("reaction-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign reaction',$1) RETURNING id",
            &[&format!("foreign-reaction-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in [creator, watcher, muted] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Reaction agent','local',$2) RETURNING id", &[&workspace,&creator]).await.unwrap().get(0);
    let other_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Foreign agent','local',$2) RETURNING id", &[&foreign,&outsider]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES($1,1,'Reaction issue','member',$2) RETURNING id", &[&workspace,&creator]).await.unwrap().get(0);
    let other_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES($1,1,'Other','member',$2) RETURNING id", &[&foreign,&outsider]).await.unwrap().get(0);
    for (user, reason) in [
        (creator, "creator"),
        (watcher, "delegated"),
        (muted, "manual"),
    ] {
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,$3)", &[&issue,&user,&reason]).await.unwrap();
    }
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES($1,$2,$3)",
        &[&workspace, &muted, &json!({"agent_activity":"muted","comments":"muted","assignments":"muted","mentions":"muted"})],
    )
    .await
    .unwrap();
    let failed_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,completed_at) VALUES($1,$2,'failed',now()) RETURNING id", &[&agent,&issue]).await.unwrap().get(0);
    let foreign_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,completed_at) VALUES($1,$2,'failed',now()) RETURNING id", &[&other_agent,&other_issue]).await.unwrap().get(0);
    let mut event = TrustedEvent {
        id: Uuid::new_v4(),
        workspace,
        issue,
        actor: Some(agent),
        actor_type: "agent".into(),
        kind: TrustedEventKind::TaskFailed { task: failed_task },
    };
    let (first, second) = tokio::join!(
        apply_trusted_event(&pool, &event),
        apply_trusted_event(&pool, &event)
    );
    let count = first.unwrap().deliveries.len() + second.unwrap().deliveries.len();
    assert_eq!(
        count, 2,
        "failure reaches creator and delegate, not muted recipient"
    );
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    let rows = db.query("SELECT recipient_id,type,severity,actor_type,actor_id FROM inbox_item WHERE issue_id=$1 AND type='task_failed'", &[&issue]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|r| [creator, watcher].contains(&r.get::<_, Uuid>(0))
                && r.get::<_, String>(1) == "task_failed"
                && r.get::<_, String>(2) == "action_required"
                && r.get::<_, Option<String>>(3).as_deref() == Some("agent")
                && r.get::<_, Option<Uuid>>(4) == Some(agent))
    );
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::TaskFailed { task: foreign_task };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.kind = TrustedEventKind::TaskFailed { task: failed_task };
    event.actor = Some(other_agent);
    assert!(matches!(
        apply_trusted_event(&pool, &event).await,
        Err(FanoutError::InvalidEvent)
    ));
    event.actor = Some(agent);
    event.kind = TrustedEventKind::TaskCompleted { task: failed_task };
    assert!(matches!(
        apply_trusted_event(&pool, &event).await,
        Err(FanoutError::InvalidEvent)
    ));
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&failed_task],
    )
    .await
    .unwrap();
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );

    let issue_reaction: Uuid = db.query_one("INSERT INTO issue_reaction(issue_id,workspace_id,actor_type,actor_id,emoji) VALUES($1,$2,'member',$3,'+1') RETURNING id", &[&issue,&workspace,&watcher]).await.unwrap().get(0);
    event.id = Uuid::new_v4();
    event.actor = Some(watcher);
    event.actor_type = "member".into();
    event.kind = TrustedEventKind::IssueReactionAdded {
        reaction: issue_reaction,
    };
    let deliveries = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].recipient, creator);
    assert_eq!(deliveries[0].recipient_type, "member");
    let row = db
        .query_one(
            "SELECT type,details FROM inbox_item WHERE id=$1",
            &[&deliveries[0].id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "reaction_added");
    assert_eq!(row.get::<_, Value>(1), json!({"emoji":"+1"}));
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.id = Uuid::new_v4();
    event.actor = Some(creator);
    assert!(matches!(
        apply_trusted_event(&pool, &event).await,
        Err(FanoutError::InvalidEvent)
    ));
    event.actor = Some(watcher);
    event.workspace = foreign;
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    event.workspace = workspace;

    let comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'text') RETURNING id", &[&issue,&workspace,&muted]).await.unwrap().get(0);
    let reaction: Uuid = db.query_one("INSERT INTO comment_reaction(comment_id,workspace_id,actor_type,actor_id,emoji) VALUES($1,$2,'member',$3,'heart') RETURNING id", &[&comment,&workspace,&watcher]).await.unwrap().get(0);
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::CommentReactionAdded { reaction };
    let deliveries = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
    assert_eq!(deliveries.len(), 1, "comments mute does not mute reactions");
    assert_eq!(deliveries[0].recipient, muted);
    let row = db
        .query_one(
            "SELECT details FROM inbox_item WHERE id=$1",
            &[&deliveries[0].id],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Value>(0),
        json!({"emoji":"heart","comment_id":comment.to_string()})
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &muted],
    )
    .await
    .unwrap();
    event.id = Uuid::new_v4();
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty(),
        "removed member cannot receive reaction"
    );
    event.issue = other_issue;
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty(),
        "foreign issue cannot receive reaction"
    );
    event.issue = issue;
    for (author_type, author, expected) in [
        ("member", watcher, 0),
        ("agent", agent, 1),
        ("system", Uuid::nil(), 0),
        ("member", outsider, 0),
        ("agent", other_agent, 0),
    ] {
        let comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES($1,$2,$3,$4,'Reaction target') RETURNING id", &[&issue,&workspace,&author_type,&author]).await.unwrap().get(0);
        let reaction: Uuid = db.query_one("INSERT INTO comment_reaction(comment_id,workspace_id,actor_type,actor_id,emoji) VALUES($1,$2,'member',$3,'+1') RETURNING id", &[&comment,&workspace,&watcher]).await.unwrap().get(0);
        event.id = Uuid::new_v4();
        event.kind = TrustedEventKind::CommentReactionAdded { reaction };
        let deliveries = apply_trusted_event(&pool, &event).await.unwrap().deliveries;
        assert_eq!(deliveries.len(), expected, "author type {author_type}");
        if expected == 1 {
            assert_eq!(deliveries[0].recipient_type, "agent");
            assert_eq!(deliveries[0].recipient, agent);
        }
    }
    // Shadow identities never acquire a human inbox, even with a dirty member row.
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&outsider],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&workspace, &outsider],
    )
    .await
    .unwrap();
    let shadow_comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'Shadow target') RETURNING id", &[&issue,&workspace,&outsider]).await.unwrap().get(0);
    let shadow_reaction: Uuid = db.query_one("INSERT INTO comment_reaction(comment_id,workspace_id,actor_type,actor_id,emoji) VALUES($1,$2,'member',$3,'+1') RETURNING id", &[&shadow_comment,&workspace,&watcher]).await.unwrap().get(0);
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::CommentReactionAdded {
        reaction: shadow_reaction,
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    // Deleted reactions are not resurrected by delayed events.
    db.execute("DELETE FROM issue_reaction WHERE id=$1", &[&issue_reaction])
        .await
        .unwrap();
    event.id = Uuid::new_v4();
    event.kind = TrustedEventKind::IssueReactionAdded {
        reaction: issue_reaction,
    };
    assert!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
    // A later inbox trigger error rolls back the entire subscriber fanout.
    db.execute(
        "UPDATE agent_task_queue SET status='failed' WHERE id=$1",
        &[&failed_task],
    )
    .await
    .unwrap();
    event.id = Uuid::new_v4();
    event.actor_type = "agent".into();
    event.actor = Some(agent);
    event.kind = TrustedEventKind::TaskFailed { task: failed_task };
    let before: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    db.batch_execute(&format!("CREATE FUNCTION channels_sprint_task_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='task_failed' AND NEW.recipient_id='{watcher}'::uuid THEN RAISE EXCEPTION 'synthetic task rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_task_abort BEFORE INSERT ON inbox_item FOR EACH ROW EXECUTE FUNCTION channels_sprint_task_abort();")).await.unwrap();
    assert!(apply_trusted_event(&pool, &event).await.is_err());
    let after: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    db.batch_execute("DROP TRIGGER channels_sprint_task_abort ON inbox_item; DROP FUNCTION channels_sprint_task_abort();").await.unwrap();
    assert_eq!(
        apply_trusted_event(&pool, &event)
            .await
            .unwrap()
            .deliveries
            .len(),
        2
    );
}
