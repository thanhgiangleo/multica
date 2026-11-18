//! Agent label assignments; catalog labels remain owned by account/catalog.
use super::*;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agents/{id}/labels", post(attach))
        .route("/api/agents/{id}/labels/{label_id}", delete(detach))
}

fn label(row: &Row) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "resource_type":row.get::<_,String>("resource_type"), "name":row.get::<_,String>("name"),
        "description":row.get::<_,String>("description"), "color":row.get::<_,String>("color"),
        "usage_count":0,
        "created_at":timestamp(row.get("created_at")), "updated_at":timestamp(row.get("updated_at")),
    })
}
async fn labels(tx: &Transaction<'_>, ws: Uuid, agent: Uuid) -> Result<GoJson<Value>, ApiError> {
    let rows = tx.query("SELECT l.* FROM issue_label l JOIN agent_to_label b ON b.label_id=l.id WHERE b.agent_id=$1 AND l.workspace_id=$2 AND l.resource_type='agent' ORDER BY lower(l.name)", &[&agent,&ws])
        .await.map_err(|_| failed("failed to list agent labels"))?;
    Ok(GoJson(
        json!({"labels":rows.iter().map(label).collect::<Vec<_>>()}),
    ))
}
#[derive(Deserialize)]
struct Attach {
    label_id: Option<String>,
}
async fn attach(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Attach>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to attach agent label"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let agent = writer(&tx, ws, user, &raw).await?;
    let request = decode(body)?;
    let raw_label = request
        .label_id
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad("label_id is required"))?;
    let label_id = parse(&raw_label, "invalid label_id")?;
    let bus = sink(&state, &actor)?;
    let row = tx.query_opt("SELECT * FROM issue_label WHERE id=$1 AND workspace_id=$2 AND resource_type='agent' FOR SHARE", &[&label_id,&ws])
        .await.map_err(|_| failed("failed to load agent label"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"agent label not found"))?;
    tx.execute(
        "INSERT INTO agent_to_label(agent_id,label_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
        &[&agent, &label_id],
    )
    .await
    .map_err(|_| failed("failed to attach agent label"))?;
    let result = labels(&tx, ws, agent).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit agent label"))?;
    publish(
        &bus,
        ws,
        user,
        "label:updated",
        json!({"label":label(&row)}),
    );
    Ok(result)
}
async fn detach(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, label)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let label_id = parse(&label, "invalid label id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to detach agent label"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let agent = writer(&tx, ws, user, &raw).await?;
    let bus = sink(&state, &actor)?;
    tx.execute(
        "DELETE FROM agent_to_label WHERE agent_id=$1 AND label_id=$2",
        &[&agent, &label_id],
    )
    .await
    .map_err(|_| failed("failed to detach agent label"))?;
    let result = labels(&tx, ws, agent).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit agent label"))?;
    publish(
        &bus,
        ws,
        user,
        "label:updated",
        json!({"label_id":label_id,"resource_type":"agent"}),
    );
    Ok(result)
}
