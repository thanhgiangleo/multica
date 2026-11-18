//! Trigger definition transactions and write-only webhook credentials.
use super::*;
use axum::routing::{delete, patch, post, put};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/autopilots/{id}/triggers", post(create))
        .route(
            "/api/autopilots/{id}/triggers/{triggerId}",
            patch(update).delete(remove),
        )
        .route(
            "/api/autopilots/{id}/triggers/{triggerId}/",
            patch(update).delete(remove),
        )
        .route(
            "/api/autopilots/{id}/triggers/{triggerId}/rotate-webhook-token",
            post(rotate),
        )
        .route(
            "/api/autopilots/{id}/triggers/{triggerId}/signing-secret",
            put(secret),
        )
        .route("/api/autopilots/{id}/collaborators", post(add_collaborator))
        .route(
            "/api/autopilots/{id}/collaborators/{userId}",
            delete(remove_collaborator),
        )
}

pub(super) fn validate_template(template: Option<&str>) -> Result<(), ApiError> {
    let regex =
        regex::Regex::new(r"\{\{\s*([^{}]*?)\s*\}\}").expect("constant template expression");
    for token in regex.captures_iter(template.unwrap_or("")) {
        if token[1].trim() != "date" {
            return Err(bad("unknown template variable; supported: {{date}}"));
        }
    }
    Ok(())
}

pub(super) fn response(row: &Row, secrets: bool) -> Value {
    let kind: String = row.get("kind");
    let token: Option<String> = if secrets && kind == "webhook" {
        row.get("webhook_token")
    } else {
        None
    };
    let signing: Option<String> = row.get("signing_secret");
    let configured = kind == "webhook" && signing.as_ref().is_some_and(|s| !s.is_empty());
    let hint = signing.as_ref().filter(|_| configured).map(|s| {
        s.chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>()
    });
    let path = token
        .as_ref()
        .filter(|s| !s.is_empty())
        .map(|s| format!("/api/webhooks/autopilots/{s}"));
    let mut result = json!({"id":row.get::<_,Uuid>("id"),"autopilot_id":row.get::<_,Uuid>("autopilot_id"),"kind":kind,"enabled":row.get::<_,bool>("enabled"),"cron_expression":row.get::<_,Option<String>>("cron_expression"),"timezone":row.get::<_,Option<String>>("timezone"),"next_run_at":time(row,"next_run_at"),"webhook_token":token,"webhook_path":path,"webhook_url":Value::Null,"has_signing_secret":configured,"signing_secret_hint":hint,"label":row.get::<_,Option<String>>("label"),"last_fired_at":time(row,"last_fired_at"),"created_at":time(row,"created_at"),"updated_at":time(row,"updated_at")});
    if kind == "webhook" {
        result["provider"] = json!(row.get::<_, String>("provider"));
        if let Some(filters) = row
            .get::<_, Option<Value>>("event_filters")
            .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
        {
            result["event_filters"] = filters;
        }
    }
    result
}

fn filters(input: &Value, kind: &str) -> Result<Option<Value>, ApiError> {
    let Some(value) = input.get("event_filters").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let entries = value
        .as_array()
        .ok_or_else(|| bad("invalid event_filters"))?;
    if kind != "webhook" && !entries.is_empty() {
        return Err(bad("event_filters is only valid for webhook triggers"));
    }
    let mut normalized = Vec::new();
    for entry in entries {
        let event = text(entry, "event").ok_or_else(|| bad("event_filters event is required"))?;
        if event.trim().is_empty() {
            return Err(bad("event_filters event must not be empty"));
        }
        let mut filter = json!({"event":event});
        if let Some(actions) = entry.get("actions").filter(|v| !v.is_null()) {
            let actions = actions
                .as_array()
                .ok_or_else(|| bad("invalid event_filters actions"))?;
            if actions
                .iter()
                .any(|a| a.as_str().is_none_or(|s| s.trim().is_empty()))
            {
                return Err(bad("event_filters actions must not be empty"));
            }
            if !actions.is_empty() {
                filter["actions"] = json!(actions);
            }
        }
        normalized.push(filter);
    }
    Ok(Some(json!(normalized)))
}

fn mint() -> Result<String, ApiError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| fail("failed to mint webhook token"))?;
    Ok(format!("awt_{}", URL_SAFE_NO_PAD.encode(bytes)))
}

async fn locked_trigger(
    tx: &Transaction<'_>,
    ws: Uuid,
    ap: Uuid,
    raw: &str,
) -> Result<Row, ApiError> {
    let id = parse_id(raw, "invalid trigger id")?;
    tx.query_opt("SELECT t.* FROM autopilot_trigger t JOIN autopilot a ON a.id=t.autopilot_id WHERE t.id=$1 AND t.autopilot_id=$2 AND a.workspace_id=$3 FOR UPDATE OF t", &[&id,&ap,&ws]).await.map_err(|_|fail("failed to load trigger"))?.ok_or_else(||error(StatusCode::NOT_FOUND,"trigger not found"))
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to create trigger"))?;
    let ap = load(&db, ws, &raw).await?;
    let publisher = required_writer(&db, &ap, ws, user, &actor).await?;
    let id: Uuid = ap.get("id");
    let input = body(&bytes)?;
    validate_strings(
        &input,
        &["kind", "cron_expression", "timezone", "label", "provider"],
    )?;
    let kind = text(&input, "kind")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad("kind is required"))?;
    if !matches!(kind, "schedule" | "webhook") {
        return Err(bad("kind must be schedule or webhook"));
    }
    let expression = optional_text(&input, "cron_expression")?;
    let zone = optional_text(&input, "timezone")?;
    let label = optional_text(&input, "label")?;
    let provider = text(&input, "provider")
        .filter(|s| !s.is_empty())
        .unwrap_or("generic");
    if !matches!(provider, "generic" | "github") {
        return Err(bad("provider must be generic or github"));
    }
    if kind != "webhook" && text(&input, "provider").is_some_and(|s| !s.is_empty()) {
        return Err(bad("provider is only valid for webhook triggers"));
    }
    if kind == "webhook" && zone.as_ref().is_some_and(|s| !s.is_empty()) {
        return Err(bad("timezone is not valid for webhook triggers"));
    }
    let event_filters = filters(&input, kind)?;
    for _ in 0..3 {
        let tx = db
            .transaction()
            .await
            .map_err(|_| fail("failed to create trigger"))?;
        let locked = load_locked(&tx, ws, id).await?;
        required_writer(&tx, &locked, ws, user, &actor).await?;
        let now: DateTime<Utc> = tx
            .query_one("SELECT now()", &[])
            .await
            .map_err(|_| fail("failed to compute schedule"))?
            .get(0);
        let (cron, zone, next, token) = if kind == "schedule" {
            let expr = expression
                .as_ref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| bad("cron_expression is required for schedule triggers"))?;
            let tz = zone.as_deref().filter(|s| !s.is_empty()).unwrap_or("UTC");
            let next = automation_sprint_cron::next(expr, tz, now)?;
            (expression.clone(), zone.clone(), Some(next), None)
        } else {
            (None, None, None, Some(mint()?))
        };
        let row=match tx.query_one("INSERT INTO autopilot_trigger(autopilot_id,kind,enabled,cron_expression,timezone,next_run_at,webhook_token,label,provider,event_filters,published_by_type,published_by_id,created_by_type,created_by_id) VALUES ($1,$2,true,$3,$4,$5,$6,$7,$8,$9,'member',$10,'member',$10) RETURNING *", &[&id,&kind,&cron,&zone,&next,&token,&label,&provider,&event_filters,&publisher]).await {
            Ok(row)=>row,
            Err(e) if kind=="webhook" && e.code()==Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)=> {tx.rollback().await.map_err(|_|fail("failed to create trigger"))?;continue;},
            Err(_)=>return Err(fail("failed to create trigger")),
        };
        rule_version(&tx, &locked, publisher).await?;
        let result = response(&row, true);
        let redacted = response(&row, false);
        tx.commit()
            .await
            .map_err(|_| fail("failed to create trigger"))?;
        emit(
            &state,
            "autopilot:updated",
            ws,
            publisher,
            json!({"autopilot_id":id,"trigger":redacted}),
        );
        return Ok((StatusCode::CREATED, GoJson(result)));
    }
    Err(fail("could not mint unique webhook token"))
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, trigger)): Path<(String, String)>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to update trigger"))?;
    let ap = load(&db, ws, &raw).await?;
    let editor = required_writer(&db, &ap, ws, user, &actor).await?;
    let id: Uuid = ap.get("id");
    let input = body(&bytes)?;
    validate_strings(&input, &["cron_expression", "timezone", "label"])?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to update trigger"))?;
    let ap = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &ap, ws, user, &actor).await?;
    let old = locked_trigger(&tx, ws, id, &trigger).await?;
    let trigger: Uuid = old.get("id");
    let kind: String = old.get("kind");
    if kind != "schedule" && input.get("cron_expression").is_some_and(|v| !v.is_null()) {
        return Err(bad("cron_expression is only valid for schedule triggers"));
    }
    if kind != "schedule" && input.get("timezone").is_some_and(|v| !v.is_null()) {
        return Err(bad("timezone is only valid for schedule triggers"));
    }
    let expr = optional_text(&input, "cron_expression")?.or_else(|| old.get("cron_expression"));
    let zone = optional_text(&input, "timezone")?.or_else(|| old.get("timezone"));
    let label = optional_text(&input, "label")?.or_else(|| old.get("label"));
    let enabled = match input.get("enabled").filter(|v| !v.is_null()) {
        None => old.get::<_, bool>("enabled"),
        Some(Value::Bool(b)) => *b,
        _ => return Err(bad("invalid request body")),
    };
    if kind != "webhook" && input.get("event_filters").is_some_and(|v| !v.is_null()) {
        return Err(bad("event_filters is only valid for webhook triggers"));
    }
    let event_filters = filters(&input, &kind)?.or_else(|| old.get("event_filters"));
    let next = if kind == "schedule" && expr.as_ref().is_some_and(|s| !s.is_empty()) {
        let now: DateTime<Utc> = tx
            .query_one("SELECT now()", &[])
            .await
            .map_err(|_| fail("failed to compute schedule"))?
            .get(0);
        Some(automation_sprint_cron::next(
            expr.as_deref().unwrap(),
            zone.as_deref().unwrap_or("UTC"),
            now,
        )?)
    } else {
        old.get::<_, Option<DateTime<Utc>>>("next_run_at")
    };
    let substantive = old.get::<_, bool>("enabled") != enabled
        || old.get::<_, Option<String>>("cron_expression") != expr
        || old.get::<_, Option<String>>("timezone") != zone
        || old.get::<_, Option<Value>>("event_filters") != event_filters;
    let row=tx.query_one("UPDATE autopilot_trigger SET enabled=$2,cron_expression=$3,timezone=$4,next_run_at=$5,label=$6,event_filters=$7,updated_at=now(),published_by_type=CASE WHEN $8 THEN 'member' ELSE published_by_type END,published_by_id=CASE WHEN $8 THEN $9 ELSE published_by_id END WHERE id=$1 RETURNING *", &[&trigger,&enabled,&expr,&zone,&next,&label,&event_filters,&substantive,&editor]).await.map_err(|_|fail("failed to update trigger"))?;
    if substantive {
        rule_version(&tx, &ap, editor).await?;
    }
    let result = response(&row, true);
    let redacted = response(&row, false);
    tx.commit()
        .await
        .map_err(|_| fail("failed to update trigger"))?;
    emit(
        &state,
        "autopilot:updated",
        ws,
        editor,
        json!({"autopilot_id":id,"trigger":redacted}),
    );
    Ok(GoJson(result))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, trigger)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to delete trigger"))?;
    let ap = load(&db, ws, &raw).await?;
    let editor = required_writer(&db, &ap, ws, user, &actor).await?;
    let id: Uuid = ap.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to delete trigger"))?;
    let ap = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &ap, ws, user, &actor).await?;
    let trigger = locked_trigger(&tx, ws, id, &trigger).await?;
    let trigger: Uuid = trigger.get("id");
    tx.execute(
        "DELETE FROM autopilot_trigger WHERE id=$1 AND autopilot_id=$2",
        &[&trigger, &id],
    )
    .await
    .map_err(|_| fail("failed to delete trigger"))?;
    rule_version(&tx, &ap, editor).await?;
    tx.commit()
        .await
        .map_err(|_| fail("failed to delete trigger"))?;
    emit(
        &state,
        "autopilot:updated",
        ws,
        editor,
        json!({"autopilot_id":id,"deleted_trigger_id":trigger}),
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn rotate(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, trigger)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    change_credential(state, ws, user, actor, raw, trigger, None).await
}
async fn secret(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, trigger)): Path<(String, String)>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let input = body(&bytes)?;
    let secret = optional_text(&input, "signing_secret")?
        .unwrap_or_default()
        .trim()
        .to_owned();
    if !secret.is_empty() && secret.len() < 16 {
        return Err(bad("signing_secret must be at least 16 characters"));
    }
    change_credential(state, ws, user, actor, raw, trigger, Some(secret)).await
}
async fn change_credential(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    trigger: String,
    secret: Option<String>,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to update webhook credential"))?;
    let ap = load(&db, ws, &raw).await?;
    let editor = required_writer(&db, &ap, ws, user, &actor).await?;
    let id: Uuid = ap.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to update webhook credential"))?;
    let ap = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &ap, ws, user, &actor).await?;
    let old = locked_trigger(&tx, ws, id, &trigger).await?;
    let trigger: Uuid = old.get("id");
    if old.get::<_, String>("kind") != "webhook" {
        return Err(bad("trigger is not a webhook trigger"));
    }
    // Go's credential endpoints update neither rule version nor publisher.
    let row = if let Some(secret) = secret {
        let signing = if secret.is_empty() {
            None
        } else {
            Some(secret)
        };
        tx.query_one("UPDATE autopilot_trigger SET signing_secret=$2,updated_at=now() WHERE id=$1 RETURNING *", &[&trigger,&signing]).await.map_err(|_|fail("failed to update webhook credential"))?
    } else {
        let mut rotated = None;
        for _ in 0..3 {
            let token = mint()?;
            tx.batch_execute("SAVEPOINT automation_sprint_rotate")
                .await
                .map_err(|_| fail("failed to rotate webhook token"))?;
            match tx.query_one("UPDATE autopilot_trigger SET webhook_token=$2,updated_at=now() WHERE id=$1 RETURNING *", &[&trigger,&token]).await {
                Ok(row)=>{rotated=Some(row);tx.batch_execute("RELEASE SAVEPOINT automation_sprint_rotate").await.map_err(|_|fail("failed to rotate webhook token"))?;break;},
                Err(e) if e.code()==Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)=>{tx.batch_execute("ROLLBACK TO SAVEPOINT automation_sprint_rotate; RELEASE SAVEPOINT automation_sprint_rotate").await.map_err(|_|fail("failed to rotate webhook token"))?;},
                Err(_)=>return Err(fail("failed to rotate webhook token")),
            }
        }
        rotated.ok_or_else(|| fail("failed to rotate webhook token"))?
    };
    let result = response(&row, true);
    let redacted = response(&row, false);
    tx.commit()
        .await
        .map_err(|_| fail("failed to update webhook credential"))?;
    emit(
        &state,
        "autopilot:updated",
        ws,
        editor,
        json!({"autopilot_id":id,"trigger":redacted}),
    );
    Ok(GoJson(result))
}

async fn add_collaborator(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let input = body(&bytes)?;
    let target = parse_id(text(&input, "user_id").unwrap_or(""), "invalid user_id")?;
    let result = collaborator(state, ws, user, actor, raw, target, true).await?;
    Ok((StatusCode::CREATED, GoJson(result)))
}
async fn remove_collaborator(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, target)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    let target = parse_id(&target, "invalid user id")?;
    Ok(GoJson(
        collaborator(state, ws, user, actor, raw, target, false).await?,
    ))
}
async fn collaborator(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    target: Uuid,
    add: bool,
) -> Result<Value, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to update collaborators"))?;
    let ap = load(&db, ws, &raw).await?;
    let (editor, role) = acting_member(&db, ws, user, &actor).await?;
    if !own(&ap, editor, &role) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the autopilot creator or a workspace admin can manage access",
        ));
    }
    let id: Uuid = ap.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to update collaborators"))?;
    // Match member removal's subscriber-identity lock before touching access rows.
    if add {
        lock_subscribers(&tx, ws, &[(target, 0)]).await?;
    }
    let ap = load_locked(&tx, ws, id).await?;
    let (editor, role) = acting_member(&tx, ws, user, &actor).await?;
    if !own(&ap, editor, &role) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the autopilot creator or a workspace admin can manage access",
        ));
    }
    if add {
        tx.execute("INSERT INTO autopilot_collaborator(autopilot_id,user_type,user_id,granted_by) VALUES ($1,'member',$2,$3) ON CONFLICT (autopilot_id,user_type,user_id) DO NOTHING", &[&id,&target,&editor]).await.map_err(|_|fail("failed to grant access"))?;
    } else {
        tx.execute("DELETE FROM autopilot_collaborator WHERE autopilot_id=$1 AND user_type='member' AND user_id=$2", &[&id,&target]).await.map_err(|_|fail("failed to revoke access"))?;
    }
    let mut entries = Vec::new();
    for row in tx
        .query(
            "SELECT * FROM autopilot_collaborator WHERE autopilot_id=$1 ORDER BY created_at",
            &[&id],
        )
        .await
        .map_err(|_| fail("failed to load collaborators"))?
    {
        entries.push(json!({"user_type":row.get::<_,String>("user_type"),"user_id":row.get::<_,Uuid>("user_id"),"granted_by":row.get::<_,Uuid>("granted_by"),"created_at":time(&row,"created_at")}));
    }
    tx.commit()
        .await
        .map_err(|_| fail("failed to update collaborators"))?;
    emit(
        &state,
        "autopilot:updated",
        ws,
        editor,
        json!({"autopilot_id":id}),
    );
    Ok(json!({"collaborators":entries}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_title_tokens_without_interpreting_literals() {
        for value in ["", "Normal title", "{{date}}", "{{ date }}", "{{{date}}}"] {
            assert!(validate_template(Some(value)).is_ok(), "{value}");
        }
        for value in ["{{issue}}", "{{ }}", "{{ date }} {{extra}}"] {
            assert!(validate_template(Some(value)).is_err(), "{value}");
        }
    }
    #[test]
    fn webhook_scope_is_narrow_and_normalized() {
        let input = json!({"event_filters":[{"event":"pull_request","actions":["opened"],"extra":"ignored"}]});
        assert_eq!(
            filters(&input, "webhook").unwrap(),
            Some(json!([{"event":"pull_request","actions":["opened"]}]))
        );
        assert!(filters(&input, "schedule").is_err());
        assert!(
            filters(
                &json!({"event_filters":[{"event":"x","actions":[""]}]}),
                "webhook"
            )
            .is_err()
        );
        assert_eq!(
            filters(&json!({"event_filters":null}), "webhook").unwrap(),
            None
        );
    }
    #[test]
    fn minted_credentials_match_go_token_shape() {
        let a = mint().unwrap();
        let b = mint().unwrap();
        assert!(a.starts_with("awt_") && a.len() == 47);
        assert_ne!(a, b);
    }
}
