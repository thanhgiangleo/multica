//! Wakeup rules that waited for this run hand it their inputs at claim time.
//!
//! Go `JoinWaitingWakeups`. Joining is a reservation, not a consume: the
//! receipts stay pending against this task, so a rule keeps its inputs until the
//! run actually starts, and gets them back if the run ends without starting.
//! What an earlier claim of the same run reserved is checked again here, so a
//! rule turned off, edited or no longer allowed drops out.
//!
//! Every failure path returns the run's context unchanged: a claim must still be
//! delivered when the wakeup bookkeeping cannot be answered.
use super::{
    ApiState,
    agent_invoke::{InvokeTarget, Invoker, can_invoke},
    attribution::Source,
    attribution_db::{self, IssueFacts},
    catalog::is_builtin_status,
    issue_system_wakeup, system_wakeup,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tokio::time::{Duration, timeout};
use tokio_postgres::GenericClient;
use tokio_postgres::Row;
use uuid::Uuid;

const CONDITION_EVENT: &str = "condition.met";
const MANUAL_EVENT: &str = "wakeup.manual";
const TIMEOUT_EVENT: &str = "wakeup.timeout";
const CHAIN_LIMIT: usize = 32;
/// A chain that already passes through the same rule twice means rules are
/// waking each other with nobody in between.
const LOOP_REPEATS: usize = 2;
const MAX_JOINED_NOTE: usize = 12000;
const OMITTED_EVIDENCE: &str = "Some trigger details were omitted to keep this prompt bounded. Read current issue comments, runs and state before deciding what to do.\n";
const JOIN_BUDGET: Duration = Duration::from_secs(2);

#[derive(Debug)]
enum JoinError {
    Database,
}

type Join<T> = Result<T, JoinError>;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct JoinedWakeup {
    #[serde(default)]
    wakeup_id: String,
    #[serde(default)]
    wakeup_revision: i64,
    #[serde(default)]
    note: String,
}

#[derive(Default, Deserialize)]
struct Stored {
    #[serde(default)]
    wakeup_joined: Vec<JoinedWakeup>,
    #[serde(default)]
    wakeup_chain: Vec<String>,
}

#[derive(Default, Deserialize)]
struct ReceiptPayload {
    #[serde(default)]
    actor_type: String,
    #[serde(default)]
    actor_id: String,
    #[serde(default)]
    agent_id: String,
    #[serde(default)]
    coalesced_count: i64,
}

#[derive(Default, Deserialize)]
struct ConditionPayload {
    #[serde(default)]
    source_task_ids: Vec<String>,
}

#[derive(Default, Deserialize)]
struct ChainRefs {
    #[serde(default)]
    source_task_id: String,
    #[serde(default)]
    task_id: String,
}

struct Receipt {
    event_type: String,
    payload: Value,
}

struct Wakeup {
    id: Uuid,
    workspace_id: Uuid,
    issue_id: Uuid,
    agent_id: Uuid,
    created_by: Uuid,
    source_task_id: Option<Uuid>,
    system_rule: Option<String>,
    instruction: String,
    mode: String,
    enabled: bool,
    disabled: bool,
    revision: i64,
    /// Go tests `len(w.Condition) > 0`, so only SQL NULL is "no condition": a
    /// JSON `null` still narrows the receipts this rule accepts.
    condition: Option<Value>,
    filter_actor_type: Option<String>,
    filter_actor_id: Option<Uuid>,
    last_task_id: Option<Uuid>,
}

struct Issue {
    id: Uuid,
    workspace_id: Uuid,
    status: String,
    assignee_type: Option<String>,
    assignee_id: Option<Uuid>,
    creator_type: String,
    creator_id: Option<Uuid>,
    origin_type: Option<String>,
    origin_id: Option<Uuid>,
}

struct AgentRow {
    id: Uuid,
    workspace_id: Uuid,
    owner_id: Option<Uuid>,
    permission_mode: String,
    runtime_id: Option<Uuid>,
    archived: bool,
}

const WAKEUP_LOCKED: &str = "SELECT id,workspace_id,issue_id,agent_id,created_by,source_task_id,system_rule,instruction,mode,enabled,disabled_at IS NOT NULL AS disabled,revision,condition,filter_actor_type,filter_actor_id,last_task_id FROM issue_wakeup WHERE id=$1 FOR UPDATE SKIP LOCKED";

fn wakeup_row(row: &Row) -> Wakeup {
    Wakeup {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        issue_id: row.get("issue_id"),
        agent_id: row.get("agent_id"),
        created_by: row.get("created_by"),
        source_task_id: row.get("source_task_id"),
        system_rule: row.get("system_rule"),
        instruction: row.get("instruction"),
        mode: row.get("mode"),
        enabled: row.get("enabled"),
        disabled: row.get("disabled"),
        revision: row.get("revision"),
        condition: row.get("condition"),
        filter_actor_type: row.get("filter_actor_type"),
        filter_actor_id: row.get("filter_actor_id"),
        last_task_id: row.get("last_task_id"),
    }
}

fn issue_row(row: &Row) -> Issue {
    Issue {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        status: row.get("status"),
        assignee_type: row.get("assignee_type"),
        assignee_id: row.get("assignee_id"),
        creator_type: row
            .get::<_, Option<String>>("creator_type")
            .unwrap_or_default(),
        creator_id: row.get("creator_id"),
        origin_type: row.get("origin_type"),
        origin_id: row.get("origin_id"),
    }
}

fn context_bytes(task: &Row) -> Vec<u8> {
    task.get::<_, Option<Value>>("context")
        .and_then(|value| serde_json::to_vec(&value).ok())
        .unwrap_or_else(|| b"{}".to_vec())
}

/// The context the claim should render, or `None` when the join could not be
/// answered and the caller must keep what the response already carried.
pub(super) async fn join_waiting_wakeups(state: &ApiState, task: &Row) -> Option<Vec<u8>> {
    let original = context_bytes(task);
    match timeout(JOIN_BUDGET, try_join(state, task, original.clone())).await {
        Ok(Ok(bytes)) => Some(bytes),
        Ok(Err(_)) | Err(_) => {
            eprintln!(
                "daemon claim: waiting wakeups keep their inputs for task {}",
                task.get::<_, Uuid>("id")
            );
            None
        }
    }
}

async fn try_join(state: &ApiState, task: &Row, original: Vec<u8>) -> Join<Vec<u8>> {
    let Some(issue_id) = task.get::<_, Option<Uuid>>("issue_id") else {
        return Ok(original);
    };
    if task.get::<_, Option<Uuid>>("originator_user_id").is_none() {
        return Ok(original);
    }
    if task.get::<_, String>("status") != "dispatched" {
        return Ok(original);
    }
    let task_id: Uuid = task.get("id");
    let agent_id: Uuid = task.get("agent_id");
    let stored: Stored = serde_json::from_slice(&original).unwrap_or_default();
    let mut db = state.pool.get().await.map_err(|_| JoinError::Database)?;
    // Read without locks; every rule is locked and rechecked below.
    let candidates: Vec<Uuid> = (&**db)
        .query(
            "SELECT w.id FROM issue_wakeup w WHERE w.issue_id=$1 AND (w.agent_id=$2 OR w.system_rule IS NOT NULL) AND w.disabled_at IS NULL AND EXISTS(SELECT 1 FROM issue_wakeup_receipt r WHERE r.wakeup_id=w.id AND r.revision=w.revision AND r.processed_at IS NULL AND (w.condition IS NULL OR r.event_type IN ('condition.met','wakeup.timeout','wakeup.manual'))) ORDER BY w.created_at,w.id",
            &[&issue_id, &agent_id],
        )
        .await
        .map_err(|_| JoinError::Database)?
        .iter()
        .map(|row| row.get(0))
        .collect();
    if candidates.is_empty() && stored.wakeup_joined.is_empty() {
        return Ok(original);
    }
    let mut previous: HashMap<Uuid, JoinedWakeup> = HashMap::new();
    let mut rules: Vec<Uuid> = Vec::new();
    for entry in &stored.wakeup_joined {
        if let Ok(id) = Uuid::parse_str(&entry.wakeup_id) {
            previous.insert(id, entry.clone());
        }
        rules.push(Uuid::parse_str(&entry.wakeup_id).unwrap_or_default());
    }
    rules.retain(|id| !id.is_nil());
    for id in candidates {
        if !previous.contains_key(&id) {
            rules.push(id);
        }
    }
    let tx = db.transaction().await.map_err(|_| JoinError::Database)?;
    tx.batch_execute("SET LOCAL lock_timeout = '50ms'")
        .await
        .map_err(|_| JoinError::Database)?;
    let txr = &*tx;
    let Some(row) = txr
        .query_opt(
            "SELECT id,workspace_id,status,assignee_type,assignee_id,creator_type,creator_id,origin_type,origin_id FROM issue WHERE id=$1",
            &[&issue_id],
        )
        .await
        .map_err(|_| JoinError::Database)?
    else {
        return Ok(original);
    };
    let issue = issue_row(&row);
    let active = wakeup_issue_active(txr, &issue).await?;
    let mut changed = previous.len() != stored.wakeup_joined.len();
    let mut joined: Vec<JoinedWakeup> = Vec::with_capacity(rules.len());
    let mut chain = stored.wakeup_chain.clone();
    for id in rules {
        let prev = previous.get(&id).cloned();
        let locked = txr
            .query_opt(WAKEUP_LOCKED, &[&id])
            .await
            .map_err(|_| JoinError::Database)?;
        let Some(row) = locked else {
            // Busy: keep what an earlier claim reserved. Deleted: nothing left.
            let still = txr
                .query_opt("SELECT id FROM issue_wakeup WHERE id=$1", &[&id])
                .await
                .map_err(|_| JoinError::Database)?;
            match (prev, still) {
                (Some(prev), Some(_)) => joined.push(prev),
                (Some(_), None) => changed = true,
                (None, _) => {}
            }
            continue;
        };
        let wakeup = wakeup_row(&row);
        let (entry, rule_chain) = reserve_for_run(txr, &issue, active, task_id, &wakeup).await?;
        let Some(entry) = entry else {
            changed = changed || prev.is_some();
            continue;
        };
        if prev.as_ref() != Some(&entry) {
            changed = true;
            chain.extend(rule_chain);
        }
        joined.push(entry);
    }
    if !changed {
        return Ok(original);
    }
    if chain.len() > CHAIN_LIMIT {
        chain = chain[chain.len() - CHAIN_LIMIT..].to_vec();
    }
    let mut values = match serde_json::from_slice::<Value>(&original) {
        Ok(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    values.insert(
        "wakeup_joined".to_owned(),
        serde_json::to_value(&joined).unwrap_or_else(|_| Value::Array(Vec::new())),
    );
    if !chain.is_empty() {
        values.insert(
            "wakeup_chain".to_owned(),
            serde_json::to_value(&chain).unwrap_or_else(|_| Value::Array(Vec::new())),
        );
    }
    let next = Value::Object(values);
    // Only the claim being answered may change its task's context.
    let updated = tx
        .query_opt(
            "UPDATE agent_task_queue SET context=$1::jsonb WHERE id=$2 AND status='dispatched' AND dispatched_at=$3 RETURNING context",
            &[
                &next,
                &task_id,
                &task.get::<_, Option<chrono::DateTime<chrono::Utc>>>("dispatched_at"),
            ],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    let Some(updated) = updated else {
        // A newer claim of this run answers instead.
        return Ok(original);
    };
    tx.commit().await.map_err(|_| JoinError::Database)?;
    Ok(updated
        .get::<_, Option<Value>>(0)
        .and_then(|value| serde_json::to_vec(&value).ok())
        .unwrap_or_else(|| b"{}".to_vec()))
}

/// Reserve one locked rule's pending inputs for this run, or release what the
/// run held and return nothing when the rule must not reach it.
async fn reserve_for_run(
    db: &impl GenericClient,
    issue: &Issue,
    active: bool,
    task_id: Uuid,
    wakeup: &Wakeup,
) -> Join<(Option<JoinedWakeup>, Vec<String>)> {
    let pending = db
        .query(
            "SELECT id,event_type,payload,task_id FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND revision=$2 AND processed_at IS NULL ORDER BY created_at,id LIMIT 100 FOR UPDATE",
            &[&wakeup.id, &wakeup.revision],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    let mut receipts = Vec::new();
    let mut held: Vec<Uuid> = Vec::new();
    let mut free: Vec<Uuid> = Vec::new();
    for row in &pending {
        let event_type: String = row.get("event_type");
        if wakeup.condition.is_some()
            && event_type != CONDITION_EVENT
            && event_type != TIMEOUT_EVENT
            && event_type != MANUAL_EVENT
        {
            continue;
        }
        match row.get::<_, Option<Uuid>>("task_id") {
            Some(owner) if owner == task_id => held.push(row.get("id")),
            None => free.push(row.get("id")),
            Some(_) => continue,
        }
        receipts.push(Receipt {
            event_type,
            payload: row
                .get::<_, Option<Value>>("payload")
                .unwrap_or(Value::Null),
        });
    }
    if receipts.is_empty() || !active || wakeup.disabled || wakeup.issue_id != issue.id {
        return release(db, held).await;
    }
    // The rule's own queued run takes its inputs; this run must not.
    let own_run = db
        .query_opt(
            "SELECT id FROM agent_task_queue WHERE context->>'wakeup_id'=$1::text AND status IN ('queued','dispatched') ORDER BY created_at LIMIT 1 FOR UPDATE",
            &[&wakeup.id.to_string()],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    if own_run.is_some() {
        return release(db, held).await;
    }
    let (instruction, may_join) = may_join(db, issue, task_id, wakeup).await?;
    if !may_join {
        return release(db, held).await;
    }
    if acknowledged_by_self(db, wakeup, task_id, issue.id, &receipts).await? {
        return release(db, held).await;
    }
    let (chain, loops) = wakeup_chain(db, wakeup, &receipts).await?;
    if loops {
        return release(db, held).await;
    }
    if !free.is_empty() {
        db.execute(
            "UPDATE issue_wakeup_receipt SET task_id=$1 WHERE id=ANY($2::uuid[]) AND processed_at IS NULL",
            &[&task_id, &free],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    }
    let note = joined_wakeup_note(wakeup, &instruction, &receipts);
    Ok((
        Some(JoinedWakeup {
            wakeup_id: wakeup.id.to_string(),
            wakeup_revision: wakeup.revision,
            note,
        }),
        chain,
    ))
}

async fn release(
    db: &impl GenericClient,
    held: Vec<Uuid>,
) -> Join<(Option<JoinedWakeup>, Vec<String>)> {
    if !held.is_empty() {
        db.execute(
            "UPDATE issue_wakeup_receipt SET task_id=NULL WHERE id=ANY($1::uuid[]) AND processed_at IS NULL",
            &[&held],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    }
    Ok((None, Vec::new()))
}

/// Whether this run is one the rule's own run would have been: the same agent,
/// run as the same person, who may still use that agent. Returns the instruction
/// the joined run receives.
async fn may_join(
    db: &impl GenericClient,
    issue: &Issue,
    task_id: Uuid,
    wakeup: &Wakeup,
) -> Join<(String, bool)> {
    let task = db
        .query_opt(
            "SELECT agent_id,originator_user_id FROM agent_task_queue WHERE id=$1",
            &[&task_id],
        )
        .await
        .map_err(|_| JoinError::Database)?;
    let Some(task) = task else {
        return Ok((String::new(), false));
    };
    let task_agent: Uuid = task.get("agent_id");
    let originator: Option<Uuid> = task.get("originator_user_id");
    let Some(agent) = agent_in_workspace(db, task_agent, wakeup.workspace_id).await? else {
        return Ok((String::new(), false));
    };
    if wakeup.system_rule.is_none() {
        if wakeup.agent_id != task_agent
            || Some(wakeup.created_by) != originator
            || (wakeup.mode == "once" && wakeup.last_task_id.is_some())
        {
            return Ok((String::new(), false));
        }
        let allowed = authorize(db, wakeup.workspace_id, wakeup.created_by, &agent).await?;
        return Ok(if allowed {
            (wakeup.instruction.clone(), true)
        } else {
            (String::new(), false)
        });
    }
    if !wakeup.enabled || effective(db, issue.workspace_id, &issue.status).await == "backlog" {
        return Ok((String::new(), false));
    }
    if resolve_wake_target(db, issue).await? != Some(task_agent) {
        return Ok((String::new(), false));
    }
    if child_done_run_as(db, issue, &agent).await? != originator {
        return Ok((String::new(), false));
    }
    // Best effort: a settings read failure falls back to the built-in text.
    let settings = db
        .query_opt(
            "SELECT settings FROM workspace WHERE id=$1",
            &[&issue.workspace_id],
        )
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<_, Value>("settings"))
        .unwrap_or(Value::Null);
    Ok((child_done_instruction(&wakeup.instruction, &settings), true))
}

/// Go `issuestatus.Effective`: a built-in keeps its key, a custom status projects
/// onto the built-in its category names, and an unresolvable key is returned
/// unchanged rather than failing the decision. Only a readable `backlog` denies,
/// so a catalog read failure must not keep the rule away from the run.
async fn effective(db: &impl GenericClient, workspace: Uuid, status: &str) -> String {
    if is_builtin_status(status) {
        return status.to_owned();
    }
    match db
        .query_opt(
            "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&workspace, &status],
        )
        .await
    {
        Ok(Some(row)) => custom_behavior(status, &row.get::<_, String>(0)),
        _ => status.to_owned(),
    }
}

fn custom_behavior(status: &str, category: &str) -> String {
    match category {
        "done" => "done".to_owned(),
        "closed" => "cancelled".to_owned(),
        _ => status.to_owned(),
    }
}

async fn wakeup_issue_active(db: &impl GenericClient, issue: &Issue) -> Join<bool> {
    let category = issue_system_wakeup::category(db, issue.workspace_id, &issue.status)
        .await
        .map_err(|_| JoinError::Database)?;
    Ok(category != "done" && category != "closed")
}

fn child_done_instruction(issue_instruction: &str, settings: &Value) -> String {
    let trimmed = issue_instruction.trim();
    if !trimmed.is_empty() {
        return trimmed.to_owned();
    }
    let (_, workspace) = system_wakeup::default(settings);
    if !workspace.is_empty() {
        return workspace;
    }
    system_wakeup::CHILD_DONE_INSTRUCTION.to_owned()
}

/// The agent a system rule reaches, when that agent can actually run.
async fn resolve_wake_target(db: &impl GenericClient, issue: &Issue) -> Join<Option<Uuid>> {
    let (Some(kind), Some(id)) = (issue.assignee_type.as_deref(), issue.assignee_id) else {
        return Ok(None);
    };
    match kind {
        "agent" => Ok(agent_in_workspace(db, id, issue.workspace_id)
            .await?
            .filter(usable)
            .map(|agent| agent.id)),
        "squad" => {
            let Some(squad) = db
                .query_opt(
                    "SELECT leader_id FROM squad WHERE id=$1 AND workspace_id=$2",
                    &[&id, &issue.workspace_id],
                )
                .await
                .map_err(|_| JoinError::Database)?
            else {
                return Ok(None);
            };
            Ok(
                agent_in_workspace(db, squad.get("leader_id"), issue.workspace_id)
                    .await?
                    .filter(usable)
                    .map(|agent| agent.id),
            )
        }
        _ => Ok(None),
    }
}

fn usable(agent: &AgentRow) -> bool {
    agent.runtime_id.is_some() && !agent.archived
}

async fn agent_in_workspace(
    db: &impl GenericClient,
    id: Uuid,
    workspace: Uuid,
) -> Join<Option<AgentRow>> {
    Ok(db
        .query_opt(
            "SELECT id,workspace_id,owner_id,permission_mode,runtime_id,archived_at IS NOT NULL AS archived FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| JoinError::Database)?
        .map(|row| AgentRow {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            owner_id: row.get("owner_id"),
            permission_mode: row.get("permission_mode"),
            runtime_id: row.get("runtime_id"),
            archived: row.get("archived"),
        }))
}

/// Go's `authorize`: the member row must exist (a read failure is an error, a
/// missing row is a denial), then the invocation policy decides.
async fn authorize(
    db: &impl GenericClient,
    workspace: Uuid,
    member: Uuid,
    agent: &AgentRow,
) -> Join<bool> {
    if agent.archived || agent.runtime_id.is_none() {
        return Ok(false);
    }
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2",
            &[&member, &workspace],
        )
        .await
        .map_err(|_| JoinError::Database)?
        .is_none()
    {
        return Ok(false);
    }
    // `CanMemberInvokeAgent` collapses a failed invocation read to a denial.
    Ok(can_invoke(
        db,
        workspace,
        InvokeTarget {
            id: agent.id,
            owner_id: agent.owner_id,
            permission_mode: &agent.permission_mode,
        },
        Invoker {
            actor_type: "member",
            actor_id: member,
            originator: None,
        },
    )
    .await
    .unwrap_or(false))
}

/// The attribution a child-done run of `issue` would run as.
async fn child_done_run_as(
    db: &impl GenericClient,
    issue: &Issue,
    agent: &AgentRow,
) -> Join<Option<Uuid>> {
    let facts = IssueFacts {
        id: issue.id,
        workspace_id: issue.workspace_id,
        creator_type: issue.creator_type.clone(),
        creator_id: issue.creator_id,
        origin_type: issue.origin_type.clone(),
        origin_id: issue.origin_id,
    };
    let attr =
        attribution_db::attribution_for_issue_task(db, &facts, None, Source::Delegation, None)
            .await;
    Ok(attribution_db::apply_attribution_fallback(
        db,
        attr,
        Some(agent.workspace_id),
        agent.owner_id,
    )
    .await
    .ok()
    .and_then(|attr| attr.user_id))
}

/// Who produced an event receipt. A coalesced receipt keeps only the latest
/// actor, so it never counts as the agent's own.
fn receipt_actor(payload: &Value) -> (String, String, bool) {
    let parsed: ReceiptPayload = serde_json::from_value(payload.clone()).unwrap_or_default();
    let single = parsed.coalesced_count <= 1;
    if parsed.actor_type.is_empty() && !parsed.agent_id.is_empty() {
        return ("agent".to_owned(), parsed.agent_id, single);
    }
    (parsed.actor_type, parsed.actor_id, single)
}

/// Whether every input of this firing came from the target agent itself, so the
/// run would only be echoing its own work back at it.
async fn acknowledged_by_self(
    db: &impl GenericClient,
    wakeup: &Wakeup,
    task_id: Uuid,
    issue: Uuid,
    receipts: &[Receipt],
) -> Join<bool> {
    if receipts.is_empty() {
        return Ok(false);
    }
    let task_agent: Uuid = match db
        .query_opt(
            "SELECT agent_id FROM agent_task_queue WHERE id=$1",
            &[&task_id],
        )
        .await
        .map_err(|_| JoinError::Database)?
    {
        Some(row) => row.get(0),
        None => return Ok(false),
    };
    let mut own_rule = wakeup.system_rule.is_some();
    if !own_rule {
        if let Some(source) = wakeup.source_task_id {
            own_rule = db
                .query_opt(
                    "SELECT agent_id FROM agent_task_queue WHERE id=$1",
                    &[&source],
                )
                .await
                .map_err(|_| JoinError::Database)?
                .is_some_and(|row| row.get::<_, Uuid>(0) == task_agent);
        }
    }
    let mut seen: HashSet<Uuid> = HashSet::new();
    let mut causes: Vec<Uuid> = Vec::new();
    for receipt in receipts {
        let event = receipt.event_type.as_str();
        if event == CONDITION_EVENT {
            if !own_rule {
                return Ok(false);
            }
            let parsed: ConditionPayload =
                serde_json::from_value(receipt.payload.clone()).unwrap_or_default();
            if parsed.source_task_ids.is_empty() {
                return Ok(false);
            }
            for raw in parsed.source_task_ids {
                let Ok(id) = Uuid::parse_str(&raw) else {
                    return Ok(false);
                };
                if seen.insert(id) {
                    causes.push(id);
                }
            }
        } else if event.starts_with("task.")
            || event == "time.due"
            || event == MANUAL_EVENT
            || event == TIMEOUT_EVENT
        {
            // A run ending, a time, a person's "wake now" and a deadline are
            // never the agent's own action.
            return Ok(false);
        } else {
            // A rule that names the agent itself as the actor asked for this.
            if wakeup.filter_actor_type.as_deref() == Some("agent")
                && wakeup.filter_actor_id == Some(task_agent)
            {
                return Ok(false);
            }
            let (actor_type, actor_id, single) = receipt_actor(&receipt.payload);
            if !single || actor_type != "agent" || actor_id != task_agent.to_string() {
                return Ok(false);
            }
        }
    }
    if causes.is_empty() {
        return Ok(true);
    }
    let active: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE id=ANY($1::uuid[]) AND agent_id=$2 AND issue_id=$3 AND status IN ('dispatched','running','waiting_local_directory')",
            &[&causes, &task_agent, &issue],
        )
        .await
        .map_err(|_| JoinError::Database)?
        .get(0);
    Ok(active as usize == causes.len())
}

/// The causal chain of rules whose runs produced this firing, oldest first and
/// ending with this rule. `true` means the chain already passes through this rule
/// often enough that another run would loop.
async fn wakeup_chain(
    db: &impl GenericClient,
    wakeup: &Wakeup,
    receipts: &[Receipt],
) -> Join<(Vec<String>, bool)> {
    let mut ids: Vec<Uuid> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for receipt in receipts {
        let refs: ChainRefs = serde_json::from_value(receipt.payload.clone()).unwrap_or_default();
        for raw in [refs.source_task_id, refs.task_id] {
            if raw.is_empty() || !seen.insert(raw.clone()) {
                continue;
            }
            if let Ok(id) = Uuid::parse_str(&raw) {
                ids.push(id);
            }
        }
    }
    let own = wakeup.id.to_string();
    let count = |chain: &[String]| chain.iter().filter(|id| id.as_str() == own).count();
    let mut upstream: Option<Vec<String>> = None;
    if !ids.is_empty() {
        let rows = db
            .query(
                "SELECT COALESCE(context->>'wakeup_id','')::text AS wakeup_id,COALESCE(context->'wakeup_chain','[]'::jsonb)::jsonb AS chain FROM agent_task_queue WHERE id=ANY($1::uuid[])",
                &[&ids],
            )
            .await
            .map_err(|_| JoinError::Database)?;
        for row in rows {
            let mut chain: Vec<String> =
                serde_json::from_value(row.get::<_, Value>("chain")).unwrap_or_default();
            let rule: String = row.get("wakeup_id");
            if !rule.is_empty() && chain.last() != Some(&rule) {
                chain.push(rule);
            }
            let better = match &upstream {
                None => true,
                Some(current) => {
                    count(&chain) > count(current)
                        || (count(&chain) == count(current) && chain.len() > current.len())
                }
            };
            if better {
                upstream = Some(chain);
            }
        }
    }
    let upstream = upstream.unwrap_or_default();
    if count(&upstream) >= LOOP_REPEATS {
        return Ok((upstream, true));
    }
    let mut chain = upstream;
    chain.push(own);
    Ok((chain, false))
}

/// What the run is told about a rule that joined it. The budget is measured in
/// bytes, as Go's `strings.Builder.Len` is, so the prompt stays under the same
/// ceiling.
fn joined_wakeup_note(wakeup: &Wakeup, instruction: &str, receipts: &[Receipt]) -> String {
    let mut note = format!(
        "Wakeup {} fired while this run was waiting to start, so it did not start a run of its own; handle it in this run. Instruction:\n{instruction}\nTrigger facts (read current state before deciding what to do):\n",
        wakeup.id
    );
    for receipt in receipts {
        let line = format!(
            "{} {}\n",
            receipt.event_type,
            canonical_wakeup_payload(&receipt.payload)
        );
        if note.len() + line.len() > MAX_JOINED_NOTE {
            note.push_str(OMITTED_EVIDENCE);
            break;
        }
        note.push_str(&line);
    }
    note
}

/// Go `canonicalWakeupPayload`: the payload re-marshalled so the note is one
/// stable line per fact, and `{}` when it cannot be read.
fn canonical_wakeup_payload(payload: &Value) -> String {
    serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_owned())
}

/// Go `JoinedWakeupNotes`: the notes a run's context carries, for its prompt.
pub(super) fn joined_wakeup_notes(context: Option<&[u8]>) -> String {
    let Some(raw) = context else {
        return String::new();
    };
    let Ok(stored) = serde_json::from_slice::<Stored>(raw) else {
        return String::new();
    };
    stored
        .wakeup_joined
        .iter()
        .map(|entry| entry.note.trim())
        .filter(|note| !note.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wakeup() -> Wakeup {
        Wakeup {
            id: Uuid::now_v7(),
            workspace_id: Uuid::nil(),
            issue_id: Uuid::nil(),
            agent_id: Uuid::nil(),
            created_by: Uuid::nil(),
            source_task_id: None,
            system_rule: None,
            instruction: "review the diff".to_owned(),
            mode: "continuous".to_owned(),
            enabled: true,
            disabled: false,
            revision: 4,
            condition: None,
            filter_actor_type: None,
            filter_actor_id: None,
            last_task_id: None,
        }
    }

    fn receipt(event: &str, payload: Value) -> Receipt {
        Receipt {
            event_type: event.to_owned(),
            payload,
        }
    }

    #[test]
    fn coalesced_receipts_are_never_the_agents_own() {
        assert_eq!(
            receipt_actor(&json!({"agent_id":"a"})),
            ("agent".into(), "a".into(), true)
        );
        assert_eq!(
            receipt_actor(&json!({"actor_type":"agent","actor_id":"a","coalesced_count":3})),
            ("agent".into(), "a".into(), false)
        );
        // An explicit actor wins over the agent fallback.
        assert_eq!(
            receipt_actor(&json!({"actor_type":"member","actor_id":"m","agent_id":"a"})),
            ("member".into(), "m".into(), true)
        );
        assert_eq!(
            receipt_actor(&Value::Null),
            (String::new(), String::new(), true)
        );
    }

    #[test]
    fn joined_note_states_the_rule_and_omits_oversized_evidence() {
        let rule = wakeup();
        let note = joined_wakeup_note(&rule, "review the diff", &[]);
        assert!(note.starts_with(&format!("Wakeup {}", rule.id)));
        assert!(note.contains("Instruction:\nreview the diff\n"));
        assert!(note.ends_with("Trigger facts (read current state before deciding what to do):\n"));
        assert!(!note.contains(OMITTED_EVIDENCE));

        let small = joined_wakeup_note(
            &rule,
            "review the diff",
            &[receipt("comment.created", json!({"actor_type":"member"}))],
        );
        assert!(small.contains("comment.created {\"actor_type\":\"member\"}\n"));
        assert!(small.len() <= MAX_JOINED_NOTE);

        let huge = "x".repeat(MAX_JOINED_NOTE);
        let bounded =
            joined_wakeup_note(&rule, "review", &[receipt("comment.created", json!(huge))]);
        assert!(bounded.ends_with(OMITTED_EVIDENCE));
        assert!(bounded.len() <= MAX_JOINED_NOTE + OMITTED_EVIDENCE.len());
    }

    #[test]
    fn joined_notes_trim_and_skip_empty_entries() {
        let raw = br#"{"wakeup_joined":[{"wakeup_id":"a","wakeup_revision":1,"note":"  first  "},{"wakeup_id":"b","wakeup_revision":1,"note":"   "},{"wakeup_id":"c","wakeup_revision":1,"note":"second"}]}"#;
        assert_eq!(joined_wakeup_notes(Some(raw)), "first\n\nsecond");
        assert_eq!(joined_wakeup_notes(Some(b"[]")), "");
        assert_eq!(joined_wakeup_notes(Some(b"not json")), "");
        assert_eq!(joined_wakeup_notes(None), "");
    }

    #[test]
    fn canonical_payload_keeps_json_and_falls_back_to_an_empty_object() {
        assert_eq!(
            canonical_wakeup_payload(&json!({"b":2,"a":1})),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(canonical_wakeup_payload(&Value::Null), "null");
    }

    #[test]
    fn terminal_custom_statuses_project_onto_built_ins() {
        assert_eq!(custom_behavior("review", "done"), "done");
        assert_eq!(custom_behavior("review", "closed"), "cancelled");
        assert_eq!(custom_behavior("review", "started"), "review");
        assert_eq!(custom_behavior("review", "unstarted"), "review");
    }

    #[test]
    fn builtin_issue_instruction_beats_the_workspace_default() {
        let settings = json!({"system_wakeup_child_done_instruction":"  workspace text  "});
        assert_eq!(
            child_done_instruction("  issue text  ", &settings),
            "issue text"
        );
        assert_eq!(child_done_instruction("   ", &settings), "workspace text");
        assert_eq!(
            child_done_instruction("", &Value::Null),
            system_wakeup::CHILD_DONE_INSTRUCTION
        );
    }
}
