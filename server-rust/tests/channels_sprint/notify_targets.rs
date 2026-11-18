use super::*;
use std::collections::HashSet;

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn direct_and_mention_workspace_privacy_and_idempotency() {
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
    let owner = insert_user(&db, "target-owner", &suffix).await;
    let member = insert_user(&db, "target-member", &suffix).await;
    let muted = insert_user(&db, "target-muted", &suffix).await;
    let squaddie = insert_user(&db, "target-squad", &suffix).await;
    let outsider = insert_user(&db, "target-outsider", &suffix).await;
    let shadow = insert_user(&db, "target-shadow", &suffix).await;
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&shadow],
    )
    .await
    .unwrap();
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Notification targets',$1) RETURNING id",
            &[&format!("targets-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign targets',$1) RETURNING id",
            &[&format!("targets-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in [owner, member, muted, squaddie, shadow] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&foreign, &outsider],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Target agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Foreign agent','local',$2) RETURNING id", &[&foreign,&outsider]).await.unwrap().get(0);
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES($1,'Target squad',$2,$3) RETURNING id", &[&workspace,&agent,&owner]).await.unwrap().get(0);
    let foreign_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES($1,'Foreign squad',$2,$3) RETURNING id", &[&foreign,&foreign_agent,&outsider]).await.unwrap().get(0);
    // A foreign squad may contain a local user's UUID, but must not resolve it.
    for (s, kind, user) in [
        (squad, "member", squaddie),
        (squad, "member", muted),
        (squad, "member", outsider),
        (squad, "agent", agent),
        (foreign_squad, "member", member),
    ] {
        db.execute(
            "INSERT INTO squad_member(squad_id,member_type,member_id) VALUES($1,$2,$3)",
            &[&s, &kind, &user],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES($1,1,'Target issue','member',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES($1,$2,$3)",
        &[
            &workspace,
            &muted,
            &json!({"mentions":"muted","assignments":"muted"}),
        ],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES($1,$2,$3)",
        &[&workspace, &member, &json!({"comments":"muted"})],
    )
    .await
    .unwrap();
    let mut n = Notification {
        event_id: Uuid::new_v4(),
        workspace,
        issue,
        actor: Some(owner),
        actor_type: "member".into(),
        excluded: HashSet::new(),
        kind: "mentioned".into(),
        severity: "info".into(),
        title: "Target issue".into(),
        body: "".into(),
        details: json!({"comment_id":Uuid::new_v4().to_string()}),
    };
    let targets = [
        ParsedMention::Member(member),
        ParsedMention::Member(member),
        ParsedMention::Member(outsider),
        ParsedMention::Member(shadow),
        ParsedMention::Squad(squad),
        ParsedMention::Squad(foreign_squad),
        ParsedMention::All,
    ];
    let rows = notify_mentions(&pool, &n, &targets).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.recipient).collect::<HashSet<_>>(),
        [member, squaddie].into_iter().collect()
    );
    assert!(
        rows.iter()
            .all(|r| r.recipient_type == "member" && r.workspace == workspace && r.issue == issue)
    );
    assert!(
        notify_mentions(&pool, &n, &targets)
            .await
            .unwrap()
            .is_empty(),
        "mention retry must not duplicate"
    );
    n.event_id = Uuid::new_v4();
    assert!(
        notify_mentions(&pool, &n, &[ParsedMention::Squad(foreign_squad)])
            .await
            .unwrap()
            .is_empty(),
        "foreign squad must not reveal local membership"
    );
    let squad_only = notify_mentions(&pool, &n, &[ParsedMention::Squad(squad)])
        .await
        .unwrap();
    assert_eq!(squad_only.len(), 1);
    assert_eq!(squad_only[0].recipient, squaddie);
    n.event_id = Uuid::new_v4();
    n.excluded.insert(member);
    n.excluded.insert(squaddie);
    assert!(
        notify_mentions(&pool, &n, &targets)
            .await
            .unwrap()
            .is_empty(),
        "actor/excluded/muted/shadow/nonmembers must be filtered"
    );
    n.excluded.clear();
    n.workspace = foreign;
    assert!(
        notify_mentions(&pool, &n, &targets)
            .await
            .unwrap()
            .is_empty(),
        "foreign issue/workspace pair must be inert"
    );
    n.workspace = workspace;
    n.kind = "issue_assigned".into();
    n.severity = "action_required".into();
    assert!(
        notify_mentions(&pool, &n, &targets).await.is_err(),
        "mention API must not turn assignment into a mention broadcast"
    );
    let direct = notify_direct(&pool, &n, DirectRecipient::Member(member))
        .await
        .unwrap();
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0].recipient_type, "member");
    assert!(
        notify_direct(&pool, &n, DirectRecipient::Member(member))
            .await
            .unwrap()
            .is_empty()
    );
    for user in [owner, muted, shadow, outsider] {
        assert!(
            notify_direct(&pool, &n, DirectRecipient::Member(user))
                .await
                .unwrap()
                .is_empty()
        );
    }
    let agent_row = notify_direct(&pool, &n, DirectRecipient::Agent(agent))
        .await
        .unwrap();
    assert_eq!(agent_row.len(), 1);
    assert_eq!(agent_row[0].recipient_type, "agent");
    assert_ne!(direct[0].id, agent_row[0].id);
    assert!(
        notify_direct(&pool, &n, DirectRecipient::Agent(foreign_agent))
            .await
            .unwrap()
            .is_empty()
    );
    // An agent UUID is not a member identity and cannot receive a mention inbox.
    n.kind = "mentioned".into();
    n.severity = "info".into();
    assert!(
        notify_mentions(&pool, &n, &[ParsedMention::Member(agent)])
            .await
            .unwrap()
            .is_empty()
    );
    // Tombstoned subscriptions do not silence direct mentions, which have their
    // own group. No new automatic subscription or task is created here.
    db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason,unsubscribed_at) VALUES($1,'member',$2,'manual',now())", &[&issue,&member]).await.unwrap();
    n.event_id = Uuid::new_v4();
    assert_eq!(
        notify_mentions(&pool, &n, &[ParsedMention::Member(member)])
            .await
            .unwrap()
            .len(),
        1
    );
    // Two concurrent workers processing the same event insert exactly one row.
    n.event_id = Uuid::new_v4();
    let shared = Arc::new(n);
    let p = pool.clone();
    let input = shared.clone();
    let one = tokio::spawn(async move {
        notify_direct(&p, &input, DirectRecipient::Member(member))
            .await
            .unwrap()
            .len()
    });
    let p = pool.clone();
    let input = shared.clone();
    let two = tokio::spawn(async move {
        notify_direct(&p, &input, DirectRecipient::Member(member))
            .await
            .unwrap()
            .len()
    });
    assert_eq!(one.await.unwrap() + two.await.unwrap(), 1);
    let mut n = Arc::try_unwrap(shared).ok().unwrap();
    // A later failure rolls back prior inserts in this same mention expansion.
    let ordered = if member < squaddie {
        [member, squaddie]
    } else {
        [squaddie, member]
    };
    db.batch_execute(&format!("CREATE FUNCTION channels_sprint_targets_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.recipient_id='{}'::uuid THEN RAISE EXCEPTION 'synthetic target rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_targets_abort BEFORE INSERT ON inbox_item FOR EACH ROW EXECUTE FUNCTION channels_sprint_targets_abort();", ordered[1])).await.unwrap();
    let before: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    n.event_id = Uuid::new_v4();
    assert!(
        notify_mentions(
            &pool,
            &n,
            &[
                ParsedMention::Member(ordered[0]),
                ParsedMention::Member(ordered[1])
            ]
        )
        .await
        .is_err()
    );
    let after: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after, "whole mention transaction must roll back");
    db.batch_execute("DROP TRIGGER channels_sprint_targets_abort ON inbox_item; DROP FUNCTION channels_sprint_targets_abort();").await.unwrap();
    assert_eq!(
        notify_mentions(
            &pool,
            &n,
            &[
                ParsedMention::Member(member),
                ParsedMention::Member(squaddie)
            ]
        )
        .await
        .unwrap()
        .len(),
        2
    );
    // Committed removal must invalidate a stale direct recipient.
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &member],
    )
    .await
    .unwrap();
    n.event_id = Uuid::new_v4();
    assert!(
        notify_direct(&pool, &n, DirectRecipient::Member(member))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        notify_mentions(&pool, &n, &[ParsedMention::Member(member)])
            .await
            .unwrap()
            .is_empty()
    );
    let forbidden: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1 AND recipient_id=ANY($2)",
            &[&issue, &vec![owner, muted, shadow, outsider, foreign_agent]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(forbidden, 0);
    let details: Value = db
        .query_one(
            "SELECT details FROM inbox_item WHERE id=$1",
            &[&direct[0].id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        details["comment_id"].is_string(),
        "string details contract retained"
    );
}
