use super::*;

pub(super) struct Fixture<'a> {
    pub pool: Arc<Pool>,
    pub health: Arc<Health>,
    pub secret: &'a [u8],
    pub token: &'a str,
    pub ws: Uuid,
    pub member: Uuid,
    pub outsider: Uuid,
    pub issue: Uuid,
    pub agent: Uuid,
    pub runtime_id: Uuid,
    pub foreign_comment: Uuid,
}

#[derive(Default)]
struct LegacyRuntime {
    pool: Option<Arc<Pool>>,
    cancelled: Mutex<Vec<CommentCancelledBatch>>,
    replays: Mutex<Vec<(Option<Uuid>, Vec<CommentReplayInput>)>>,
    changed: Mutex<Vec<CommentChangedAction>>,
    sequence: Mutex<Vec<&'static str>>,
}

#[async_trait]
impl IssueCommentRuntime for LegacyRuntime {
    async fn created(&self, _: CommentCreatedAction) -> CommentFeedback {
        panic!("unexpected create")
    }
    async fn changed(&self, action: CommentChangedAction) -> CommentFeedback {
        self.sequence.lock().unwrap().push("changed");
        self.changed.lock().unwrap().push(action);
        CommentFeedback::default()
    }
    fn legacy_batches(&self) -> Option<&dyn IssueCommentLegacyRuntime> {
        Some(self)
    }
}

#[async_trait]
impl IssueCommentLegacyRuntime for LegacyRuntime {
    async fn cancelled(&self, batch: &CommentCancelledBatch) {
        let db = self.pool.as_ref().unwrap().get().await.unwrap();
        for task in &batch.tasks {
            let status: String = db
                .query_one(
                    "SELECT status FROM agent_task_queue WHERE id=$1",
                    &[&task.id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(status, "cancelled", "broadcast must follow commit");
        }
        self.sequence.lock().unwrap().push("cancelled");
        self.cancelled.lock().unwrap().push(batch.clone());
    }
    async fn replay(
        &self,
        _: &CommentCancelledBatch,
        exclude: Option<Uuid>,
        inputs: Vec<CommentReplayInput>,
    ) {
        self.sequence.lock().unwrap().push("replay");
        self.replays.lock().unwrap().push((exclude, inputs));
    }
}

async fn comment(
    db: &deadpool_postgres::Client,
    fixture: &Fixture<'_>,
    content: &str,
    parent: Option<Uuid>,
) -> Uuid {
    db.query_one(
        "INSERT INTO comment(workspace_id,issue_id,parent_id,author_type,author_id,content,type) VALUES($1,$2,$3,'member',$4,$5,'comment') RETURNING id",
        &[&fixture.ws, &fixture.issue, &parent, &fixture.member, &content],
    ).await.unwrap().get(0)
}

async fn task(
    db: &deadpool_postgres::Client,
    f: &Fixture<'_>,
    trigger: Uuid,
    coalesced: Vec<Uuid>,
) -> Uuid {
    db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,trigger_comment_id,coalesced_comment_ids,status) VALUES($1,$2,$3,$4,$5,'queued') RETURNING id",
        &[&f.agent, &f.runtime_id, &f.issue, &trigger, &coalesced],
    ).await.unwrap().get(0)
}

pub(super) async fn run(f: Fixture<'_>) {
    let db = f.pool.get().await.unwrap();
    let runtime = Arc::new(LegacyRuntime {
        pool: Some(f.pool.clone()),
        ..Default::default()
    });
    let sink = Arc::new(Sink::default());
    let app = router_with_api(
        f.health.clone(),
        ApiState::new(f.pool.clone(), f.secret.to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_comment_runtime(runtime.clone()),
    );
    let edit_path = |id: Uuid| format!("/api/comments/{id}?workspace_id={}", f.ws);
    let main = comment(&db, &f, "original", None).await;
    let survivor = comment(&db, &f, "survivor", None).await;
    let note = comment(&db, &f, "/NOTE do not route", None).await;
    let recovery: Uuid = db.query_one(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,type,source_task_id) VALUES($1,$2,'system',$3,'recovery','progress_update',$4) RETURNING id",
        &[&f.ws,&f.issue,&f.member,&Uuid::new_v4()],
    ).await.unwrap().get(0);
    let planned = vec![survivor, survivor, note, recovery, f.foreign_comment];
    let queued = task(&db, &f, main, planned).await;
    let outsider = encode(
        &Header::default(),
        &json!({"sub":f.outsider,"exp":4102444800u64}),
        &EncodingKey::from_secret(f.secret),
    )
    .unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(main),
            &outsider,
            json!({"content":"unauthorized"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(main),
            f.token,
            json!({"content":"invalid","suppress_agent_ids":["bad"]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&queued]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "queued"
    );
    assert!(runtime.cancelled.lock().unwrap().is_empty());

    let result = call(
        &app,
        "PUT",
        &edit_path(main),
        f.token,
        json!({"content":"edited legacy"}),
    )
    .await;
    assert_eq!(result.0, StatusCode::OK, "{result:?}");
    assert_eq!(result.1["content"], "edited legacy");
    let batch = runtime.cancelled.lock().unwrap()[0].clone();
    assert_eq!(batch.task_ids, vec![queued]);
    assert_eq!(
        batch.tasks[0].coalesced_comment_ids,
        vec![survivor, survivor, note, recovery, f.foreign_comment]
    );
    let (excluded, inputs) = runtime.replays.lock().unwrap()[0].clone();
    assert_eq!(excluded, Some(main));
    assert_eq!(
        inputs.iter().map(|c| c.comment_id).collect::<Vec<_>>(),
        vec![survivor, recovery]
    );
    assert_eq!(inputs[0].target_agent_ids, vec![f.agent]);
    assert_eq!(inputs[0].author_id, f.member);
    assert!(inputs[1].delegated_failure_recovery);
    assert_eq!(
        *runtime.sequence.lock().unwrap(),
        vec!["cancelled", "replay", "changed"]
    );
    assert!(
        runtime.changed.lock().unwrap()[0]
            .cancelled_task_ids
            .is_empty(),
        "pre-committed batch must not broadcast twice"
    );
    assert_eq!(sink.changed.lock().unwrap().len(), 1);

    db.batch_execute("CREATE FUNCTION zbsi66_comment_edit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.content='__legacy_fail__' THEN RAISE EXCEPTION 'synthetic edit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi66_comment_edit_fail BEFORE UPDATE ON comment FOR EACH ROW EXECUTE FUNCTION zbsi66_comment_edit_fail()").await.unwrap();
    let failure = comment(&db, &f, "failure-original", None).await;
    let fail_task = task(&db, &f, failure, vec![survivor]).await;
    let before: i64 = db
        .query_one("SELECT revision FROM comment WHERE id=$1", &[&failure])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(failure),
            f.token,
            json!({"content":"__legacy_fail__"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&fail_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    let unchanged = db
        .query_one(
            "SELECT revision,content FROM comment WHERE id=$1",
            &[&failure],
        )
        .await
        .unwrap();
    assert_eq!(unchanged.get::<_, i64>(0), before);
    assert_eq!(unchanged.get::<_, String>(1), "failure-original");
    let (exclude, restored) = runtime.replays.lock().unwrap().last().unwrap().clone();
    assert_eq!(exclude, None);
    assert_eq!(
        restored.iter().map(|c| c.comment_id).collect::<Vec<_>>(),
        vec![survivor, failure]
    );
    let strict = comment(&db, &f, "strict-original", None).await;
    let strict_task = task(&db, &f, strict, vec![survivor]).await;
    let strict_revision: i64 = db
        .query_one("SELECT revision FROM comment WHERE id=$1", &[&strict])
        .await
        .unwrap()
        .get(0);
    let callback_count = runtime.sequence.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(strict),
            f.token,
            json!({"content":"stale","expected_revision":strict_revision+1})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(strict),
            f.token,
            json!({"content":"__legacy_fail__","expected_revision":strict_revision})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&strict_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "queued"
    );
    assert_eq!(runtime.sequence.lock().unwrap().len(), callback_count);
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(strict),
            f.token,
            json!({"content":"strict-success","expected_revision":strict_revision})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        runtime.sequence.lock().unwrap()[callback_count..],
        ["changed"]
    );
    assert_eq!(
        runtime
            .changed
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .cancelled_task_ids,
        vec![strict_task]
    );
    db.batch_execute("DROP TRIGGER zbsi66_comment_edit_fail ON comment; DROP FUNCTION zbsi66_comment_edit_fail()").await.unwrap();

    let deletion = comment(&db, &f, "delete-original", None).await;
    let reply = comment(&db, &f, "keep-reply", Some(deletion)).await;
    let delete_task = task(&db, &f, deletion, vec![survivor, recovery]).await;
    let alias = format!(
        "/api/comments/{deletion}/keep-replies?workspace_id={}",
        f.ws
    );
    assert_eq!(
        call(&app, "DELETE", &alias, f.token, json!({})).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&delete_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    assert!(
        db.query_one(
            "SELECT deleted_at IS NOT NULL FROM comment WHERE id=$1",
            &[&deletion]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert!(
        db.query_opt("SELECT 1 FROM comment WHERE id=$1", &[&reply])
            .await
            .unwrap()
            .is_some()
    );
    let (exclude, kept) = runtime.replays.lock().unwrap().last().unwrap().clone();
    assert_eq!(exclude, Some(deletion));
    assert_eq!(
        kept.iter().map(|c| c.comment_id).collect::<Vec<_>>(),
        vec![survivor, recovery]
    );
    assert!(runtime.changed.lock().unwrap().last().unwrap().deleted);

    let rollback = comment(&db, &f, "cancel-rollback", None).await;
    let rollback_task = task(&db, &f, rollback, vec![survivor]).await;
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES($1,$2,$3,$4,$5,$6,'pending')", &[&rollback_task,&f.ws,&f.issue,&Uuid::new_v4(),&f.member,&Uuid::new_v4()]).await.unwrap();
    db.batch_execute("CREATE FUNCTION zbsi66_settlement_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic settlement failure'; END $$; CREATE TRIGGER zbsi66_settlement_fail BEFORE UPDATE ON task_supplement FOR EACH ROW EXECUTE FUNCTION zbsi66_settlement_fail()").await.unwrap();
    let callback_count = runtime.sequence.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit_path(rollback),
            f.token,
            json!({"content":"must-not-commit"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&rollback_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "queued"
    );
    assert_eq!(
        db.query_one("SELECT content FROM comment WHERE id=$1", &[&rollback])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cancel-rollback"
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM task_supplement WHERE task_id=$1",
            &[&rollback_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert_eq!(runtime.sequence.lock().unwrap().len(), callback_count);
    db.batch_execute("DROP TRIGGER zbsi66_settlement_fail ON task_supplement; DROP FUNCTION zbsi66_settlement_fail()").await.unwrap();

    let rejected = comment(&db, &f, "inflight", None).await;
    let rejected_task = task(&db, &f, rejected, vec![survivor]).await;
    let unrelated = task(&db, &f, survivor, vec![]).await;
    let receipt = Uuid::new_v4();
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$6,'delivering')", &[&unrelated,&f.ws,&f.issue,&rejected,&f.member,&receipt]).await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &edit_path(rejected), f.token, json!({}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&rejected_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    assert!(
        db.query_opt("SELECT id FROM comment WHERE id=$1", &[&rejected])
            .await
            .unwrap()
            .is_some()
    );
    let (exclude, restored) = runtime.replays.lock().unwrap().last().unwrap().clone();
    assert_eq!(exclude, None);
    assert_eq!(
        restored.iter().map(|c| c.comment_id).collect::<Vec<_>>(),
        vec![survivor, rejected]
    );
}
