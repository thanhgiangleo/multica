//! Product-owned Mika provisioning and per-member onboarding conversations.
use super::*;

#[derive(Deserialize)]
pub(super) struct Request {
    #[serde(default)]
    runtime_id: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    session_title: String,
}

fn description(language: &str) -> Result<&'static str, ApiError> {
    match language {
        "en" => Ok(
            "Your workspace Chief of Staff. Mika turns goals into issues, coordinates agents, and helps build reusable workflows.",
        ),
        "zh" => Ok(
            "你的工作区 Chief of Staff。Mika 会把目标转化为任务、协调智能体，并帮你建立可复用的工作流。",
        ),
        "ko" => Ok(
            "워크스페이스의 Chief of Staff입니다. Mika가 목표를 태스크로 구체화하고 에이전트를 조율하며 재사용 가능한 워크플로 구성을 돕습니다.",
        ),
        "ja" => Ok(
            "ワークスペースの Chief of Staff。Mika は目標をタスクに落とし込み、エージェントを調整し、再利用できるワークフローづくりを支援します。",
        ),
        _ => Err(bad("language must be en, zh, ko, or ja")),
    }
}

async fn lookup(db: &impl GenericClient, ws: Uuid) -> Result<Option<Uuid>, ApiError> {
    Ok(db.query_opt("SELECT id FROM agent WHERE workspace_id=$1 AND system_key='mika' AND archived_at IS NULL ORDER BY created_at,id LIMIT 1", &[&ws])
        .await.map_err(|_| failed("failed to look up the workspace agent"))?
        .map(|row| row.get(0)))
}

async fn session(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    agent: Uuid,
    title: &str,
) -> Result<Value, ApiError> {
    let err = || failed("failed to open the Mika conversation");
    let mut db = state.pool.get().await.map_err(|_| err())?;
    let tx = db.transaction().await.map_err(|_| err())?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&format!("mika-session:{ws}:{user}")],
    )
    .await
    .map_err(|_| err())?;
    tx.query_one("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| err())?;
    let row = match tx.query_opt("SELECT * FROM chat_session WHERE workspace_id=$1 AND creator_id=$2 AND agent_id=$3 AND status='active' ORDER BY created_at,id LIMIT 1", &[&ws,&user,&agent])
        .await.map_err(|_| err())? {
        Some(row) => row,
        None => tx.query_one("INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,title,runtime_id,explicitly_created_at) VALUES ($1,$2,$3,$4,$5,(SELECT runtime_id FROM agent WHERE id=$3),now()) RETURNING *", &[&Uuid::now_v7(),&ws,&agent,&user,&title.trim()])
            .await.map_err(|_| err())?,
    };
    let response = json!(crate::api::runtime_chat::session(&row));
    tx.commit().await.map_err(|_| err())?;
    // Mika's embedded response uses the plain ChatSessionResponse contract,
    // not the enriched public chat-list projection.
    Ok(json!({
        "id":response["id"], "workspace_id":response["workspace_id"],
        "agent_id":response["agent_id"], "creator_id":response["creator_id"],
        "project_id":response["project_id"], "title":response["title"],
        "status":response["status"], "pinned":response["pinned"],
        "has_unread":false, "unread_count":0, "last_message":null,
        "created_at":response["created_at"], "updated_at":response["updated_at"]
    }))
}

pub(super) async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<Request>, JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    actor.require_human()?;
    let req = decode(body)?;
    let description = description(&req.language)?;
    let runtime = parse(&req.runtime_id, "invalid runtime_id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to look up the workspace agent"))?;
    let existing = lookup(&**db, ws).await?;
    let mut created = false;
    let agent = if let Some(id) = existing {
        id
    } else {
        let tx = db
            .transaction()
            .await
            .map_err(|_| failed("failed to start agent create transaction"))?;
        member(&tx, ws, user).await?;
        let rt = tx.query_opt("SELECT owner_id,visibility,runtime_mode,status FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE", &[&runtime,&ws])
            .await.map_err(|_| failed("failed to resolve runtime"))?
            .ok_or_else(|| bad("runtime not found in this workspace"))?;
        let owner: Option<Uuid> = rt.get("owner_id");
        if owner.is_none()
            || !(owner == Some(user) || rt.get::<_, String>("visibility") == "public")
        {
            return Err(error(
                StatusCode::FORBIDDEN,
                "you cannot bind an agent to this runtime",
            ));
        }
        let bus = sink(&state, &actor)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&format!("mika:{ws}")],
        )
        .await
        .map_err(|_| failed("failed to lock the workspace agent"))?;
        if let Some(id) = lookup(&*tx, ws).await? {
            id
        } else {
            let status = if rt.get::<_, String>("status") == "online" {
                "idle"
            } else {
                "offline"
            };
            let model = req.model.trim();
            let model = if model.is_empty() { None } else { Some(model) };
            let id: Uuid = tx.query_one("INSERT INTO agent(workspace_id,name,description,avatar_url,runtime_mode,runtime_id,model,visibility,permission_mode,max_concurrent_tasks,owner_id,instructions,kind,system_key,status) VALUES ($1,'Mika',$2,$3,$4,$5,$6,'workspace','public_to',3,$7,'','user','mika',$8) RETURNING id", &[&ws,&description,&"emoji:🦄",&rt.get::<_, String>("runtime_mode"),&runtime,&model,&user,&status])
                .await.map_err(|_| failed("failed to create the workspace agent"))?.get(0);
            tx.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id,created_by) VALUES ($1,'workspace',$2,$3)", &[&id,&ws,&user])
                .await.map_err(|_| failed("failed to save agent access"))?;
            created = true;
            // Broadcast only after commit. A subsequent session failure can be
            // retried without provisioning a second agent.
            tx.commit()
                .await
                .map_err(|_| failed("failed to commit agent create"))?;
            let tx = db
                .transaction()
                .await
                .map_err(|_| failed("failed to load agent response"))?;
            let payload = settings::broadcast(&tx, ws, id).await?;
            tx.commit()
                .await
                .map_err(|_| failed("failed to load agent response"))?;
            publish(&bus, ws, user, "agent:created", payload);
            id
        }
    };
    // Release the provisioning connection before taking the session lock.
    drop(db);
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to load agent response"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to load agent response"))?;
    let mut agent_response = settings::broadcast(&tx, ws, agent).await?["agent"].clone();
    tx.commit()
        .await
        .map_err(|_| failed("failed to load agent response"))?;
    drop(db);
    agent_response["onboarding_session"] =
        session(&state, ws, user, agent, &req.session_title).await?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        GoJson(agent_response),
    ))
}
