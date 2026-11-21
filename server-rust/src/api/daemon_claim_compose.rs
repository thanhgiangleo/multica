//! DB-free pieces of Go's claimed-task response composition.
//!
//! `buildClaimedTaskResponse` is a 1400-line builder; these are the parts that
//! carry no I/O and therefore can be exercised without a database. Faithfulness
//! to Go matters more than shape here: each function is a transcription of one
//! helper in `internal/handler`, including its passthrough and degradation
//! rules, because a claim payload is consumed by daemons that are already
//! installed.
use super::daemon::DaemonIdentity;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

/// Go `hasManagedJSON`: an empty document is "no managed config", so the caller
/// must pass the other side through untouched rather than emit `{}`.
fn has_managed_json(raw: Option<&[u8]>) -> bool {
    let Some(bytes) = raw else { return false };
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text.trim();
    !trimmed.is_empty() && trimmed != "null" && trimmed != "{}"
}

/// Go `daemonRuntimeBindingAllowed`. The typed identity replaces the
/// `x-user-id`/daemon context headers Go reads: a bound daemon token never gets
/// a user header, so the binding check must come from the token itself.
pub(super) fn daemon_runtime_binding_allowed(
    identity: &DaemonIdentity,
    daemon_id: Option<&str>,
    owner_id: Option<Uuid>,
) -> bool {
    match identity {
        DaemonIdentity::Bound {
            daemon: token_daemon,
            owner: token_owner,
            ..
        } => {
            if token_daemon.is_empty() {
                return false;
            }
            let Some(runtime_daemon) = daemon_id.filter(|id| !id.trim().is_empty()) else {
                return false;
            };
            if runtime_daemon != token_daemon {
                return false;
            }
            if token_daemon.starts_with("qoder-managed-") {
                return true;
            }
            token_owner.is_some() && owner_id.is_some() && token_owner == &owner_id
        }
        DaemonIdentity::User(user) => Some(*user) == owner_id,
    }
}

/// Go `service.IssueIdentifier`: an empty workspace prefix renders as `#N`.
pub(super) fn issue_identifier(prefix: &str, number: i64) -> String {
    if prefix.is_empty() {
        format!("#{number}")
    } else {
        format!("{prefix}-{number}")
    }
}

/// Go `parseRuntimeConnectedAppsForClaim`: the column holds a JSON array, and
/// anything unreadable degrades to "no connected apps" instead of failing the
/// claim.
pub(super) fn parse_runtime_connected_apps_for_claim(
    raw: Option<&[u8]>,
    task_id: Uuid,
) -> Option<Value> {
    let bytes = raw?;
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return None;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(value @ Value::Array(_)) => Some(value),
        _ => {
            eprintln!("claim: task {task_id} has unreadable runtime_connected_apps; omitting");
            None
        }
    }
}

/// Split a managed MCP document into its normalised `mcpServers` map plus the
/// top-level keys it must keep. Go accepts both `mcpServers` and `mcp`, with the
/// canonical container winning a collision.
fn side_servers(doc: &Map<String, Value>) -> Map<String, Value> {
    let mut servers = Map::new();
    for container in ["mcp", "mcpServers"] {
        if let Some(Value::Object(entries)) = doc.get(container) {
            for (name, config) in entries {
                servers.insert(name.clone(), config.clone());
            }
        }
    }
    servers
}

fn rest_keys(doc: &Map<String, Value>) -> Map<String, Value> {
    let mut rest = Map::new();
    for (key, value) in doc {
        if key != "mcp" && key != "mcpServers" {
            rest.insert(key.clone(), value.clone());
        }
    }
    rest
}

fn render(rest: Map<String, Value>, servers: Map<String, Value>) -> Value {
    let mut out = rest;
    if !servers.is_empty() {
        out.insert("mcpServers".into(), Value::Object(servers));
    }
    Value::Object(out)
}

fn parse_doc(raw: &[u8]) -> Option<Map<String, Value>> {
    let text = String::from_utf8_lossy(raw);
    match serde_json::from_str::<Value>(text.trim()) {
        Ok(Value::Object(doc)) => Some(doc),
        _ => None,
    }
}

/// Go `ResolveAgentMcpConfig`. Precedence is workspace bindings < the agent's
/// own servers < the per-task overlay (applied by `merge_mcp_overlay`).
/// An unreadable agent document is passed through unchanged: the daemon already
/// understands its own config better than a lossy rewrite would.
pub(super) fn resolve_agent_mcp_config(
    bindings: &[(String, Value)],
    agent_mcp_config: Option<&[u8]>,
) -> Option<Value> {
    let mut shared = Map::new();
    for (name, config) in bindings {
        if name.is_empty() || !has_managed_json(Some(config.to_string().as_bytes())) {
            continue;
        }
        shared.insert(name.clone(), config.clone());
    }
    if shared.is_empty() {
        return agent_mcp_config.and_then(|raw| parse_doc(raw).map(Value::Object));
    }
    let Some(raw) = agent_mcp_config.filter(|raw| has_managed_json(Some(raw))) else {
        let mut out = Map::new();
        out.insert("mcpServers".into(), Value::Object(shared));
        return Some(Value::Object(out));
    };
    let Some(doc) = parse_doc(raw) else {
        eprintln!("resolve agent mcp_config: parse agent mcp_config: invalid JSON object");
        return parse_doc(raw).map(Value::Object);
    };
    let mut servers = shared;
    for (name, config) in side_servers(&doc) {
        servers.insert(name, config);
    }
    Some(render(rest_keys(&doc), servers))
}

/// Go `mergeMCPOverlay`: the per-task overlay wins every collision and the
/// agent document's other top-level keys survive.
pub(super) fn merge_mcp_overlay(
    agent_mcp_config: Option<&[u8]>,
    overlay: Option<&[u8]>,
) -> Option<Value> {
    let Some(overlay_raw) = overlay.filter(|raw| has_managed_json(Some(raw))) else {
        return agent_mcp_config.and_then(|raw| parse_doc(raw).map(Value::Object));
    };
    let Some(overlay_doc) = parse_doc(overlay_raw) else {
        return agent_mcp_config.and_then(|raw| parse_doc(raw).map(Value::Object));
    };
    let Some(agent_raw) = agent_mcp_config.filter(|raw| has_managed_json(Some(raw))) else {
        return Some(render(rest_keys(&overlay_doc), side_servers(&overlay_doc)));
    };
    let Some(agent_doc) = parse_doc(agent_raw) else {
        eprintln!("merge mcp overlay: parse agent mcp_config: invalid JSON object");
        return parse_doc(agent_raw).map(Value::Object);
    };
    let mut servers = side_servers(&agent_doc);
    for (name, config) in side_servers(&overlay_doc) {
        servers.insert(name, config);
    }
    Some(render(rest_keys(&agent_doc), servers))
}

/// Go `trailingUserMessages`: only the newest uninterrupted run of user turns is
/// replayed, and it stops at the first message whose channel media is still
/// uploading — sending that one would hand the agent a dangling attachment.
/// The claim path does not replay history yet, so the helper stays for the
/// caller that adopts it.
#[allow(dead_code)]
pub(super) fn trailing_user_messages(rows: &[Value], now: DateTime<Utc>) -> Vec<Value> {
    let mut start = 0;
    for index in (0..rows.len()).rev() {
        if rows[index].get("role").and_then(Value::as_str) != Some("user") {
            start = index + 1;
            break;
        }
    }
    let tail = &rows[start..];
    let cut = tail
        .iter()
        .position(|row| {
            row.get("channel_media_pending_until")
                .and_then(Value::as_str)
                .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
                .is_some_and(|pending| pending.with_timezone(&Utc) > now)
        })
        .unwrap_or(tail.len());
    tail[..cut].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound(daemon: &str, owner: Option<Uuid>) -> DaemonIdentity {
        DaemonIdentity::Bound {
            workspace: Uuid::nil(),
            daemon: daemon.into(),
            owner,
            hash: "h".into(),
        }
    }

    #[test]
    fn binding_requires_matching_daemon_and_owner() {
        let owner = Uuid::now_v7();
        let other = Uuid::now_v7();
        assert!(daemon_runtime_binding_allowed(
            &DaemonIdentity::User(owner),
            None,
            Some(owner)
        ));
        assert!(!daemon_runtime_binding_allowed(
            &DaemonIdentity::User(owner),
            None,
            Some(other)
        ));
        assert!(daemon_runtime_binding_allowed(
            &bound("d1", Some(owner)),
            Some("d1"),
            Some(owner)
        ));
        assert!(!daemon_runtime_binding_allowed(
            &bound("d1", Some(owner)),
            Some("d2"),
            Some(owner)
        ));
        assert!(!daemon_runtime_binding_allowed(
            &bound("d1", Some(owner)),
            None,
            Some(owner)
        ));
        // A qoder-managed token is authorised by the daemon id alone; the
        // runtime owner is filled in later by the managed connection.
        assert!(daemon_runtime_binding_allowed(
            &bound("qoder-managed-1", None),
            Some("qoder-managed-1"),
            Some(other)
        ));
    }

    #[test]
    fn issue_identifier_matches_go_prefix_rule() {
        assert_eq!(issue_identifier("", 7), "#7");
        assert_eq!(issue_identifier("ZBSI", 7), "ZBSI-7");
    }

    #[test]
    fn connected_apps_degrade_to_absent() {
        let id = Uuid::now_v7();
        assert_eq!(parse_runtime_connected_apps_for_claim(None, id), None);
        assert_eq!(
            parse_runtime_connected_apps_for_claim(Some(b"null"), id),
            None
        );
        assert_eq!(
            parse_runtime_connected_apps_for_claim(Some(b"  "), id),
            None
        );
        assert_eq!(
            parse_runtime_connected_apps_for_claim(Some(b"{oops"), id),
            None
        );
        assert_eq!(
            parse_runtime_connected_apps_for_claim(Some(br#"[{"app":"gmail"}]"#), id),
            Some(serde_json::json!([{"app": "gmail"}]))
        );
    }

    #[test]
    fn resolve_mcp_config_precedence_is_workspace_then_agent() {
        let bindings = vec![
            ("shared".to_string(), serde_json::json!({"command": "ws"})),
            ("".to_string(), serde_json::json!({"command": "nameless"})),
            ("empty".to_string(), serde_json::json!({})),
        ];
        // No bindings means the agent document is returned verbatim.
        let agent = br#"{"mcpServers":{"own":{"command":"a"}},"other":1}"#;
        assert_eq!(
            resolve_agent_mcp_config(&[], Some(agent)),
            Some(serde_json::json!({"mcpServers": {"own": {"command": "a"}}, "other": 1}))
        );
        // A clean agent document without managed JSON is wrapped, not replaced.
        assert_eq!(
            resolve_agent_mcp_config(&bindings, None),
            Some(serde_json::json!({"mcpServers": {"shared": {"command": "ws"}}}))
        );
        // The agent's own server wins a name collision with a binding.
        let colliding = br#"{"mcp":{"shared":{"command":"agent"}}}"#;
        assert_eq!(
            resolve_agent_mcp_config(&bindings, Some(colliding)),
            Some(serde_json::json!({"mcpServers": {"shared": {"command": "agent"}}}))
        );
        // `mcpServers` is canonical: it overrides a `mcp` entry of the same name.
        let both =
            br#"{"mcp":{"x":{"command":"legacy"}},"mcpServers":{"x":{"command":"canonical"}}}"#;
        assert_eq!(
            resolve_agent_mcp_config(&[], Some(both)).unwrap()["mcpServers"]["x"]["command"],
            "canonical"
        );
    }

    #[test]
    fn overlay_wins_and_keeps_other_agent_keys() {
        let agent =
            br#"{"mcpServers":{"a":{"command":"agent"},"b":{"command":"agent"}},"model":"m"}"#;
        let overlay = br#"{"mcpServers":{"b":{"command":"task"}}}"#;
        let merged = merge_mcp_overlay(Some(agent), Some(overlay)).unwrap();
        assert_eq!(merged["mcpServers"]["a"]["command"], "agent");
        assert_eq!(merged["mcpServers"]["b"]["command"], "task");
        assert_eq!(merged["model"], "m");
        // No managed overlay passes the agent document through untouched.
        assert_eq!(
            merge_mcp_overlay(Some(agent), Some(b"{}")),
            Some(
                serde_json::json!({"mcpServers": {"a": {"command": "agent"}, "b": {"command": "agent"}}, "model": "m"})
            )
        );
        // No agent document leaves the overlay alone.
        assert_eq!(
            merge_mcp_overlay(None, Some(overlay)),
            Some(serde_json::json!({"mcpServers": {"b": {"command": "task"}}}))
        );
    }

    #[test]
    fn trailing_user_messages_stops_at_non_user_and_pending_media() {
        let now = Utc::now();
        let early = serde_json::json!({"role": "assistant", "id": 1});
        let user_ok = serde_json::json!({"role": "user", "id": 2});
        let user_pending = serde_json::json!({"role": "user", "id": 3, "channel_media_pending_until": "2999-01-01T00:00:00Z"});
        let user_after = serde_json::json!({"role": "user", "id": 4});
        let rows = vec![
            early.clone(),
            user_ok.clone(),
            user_pending,
            user_after.clone(),
        ];
        // The trailing run starts after the assistant row, then truncates at the
        // first message still waiting on channel media.
        let kept = trailing_user_messages(&rows, now);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0]["id"], 2);
        assert!(trailing_user_messages(&[], now).is_empty());
        // With nothing pending the whole trailing run survives.
        let clean = vec![early, user_ok, user_after.clone()];
        assert_eq!(trailing_user_messages(&clean, now).len(), 2);
        // An already-elapsed pending deadline does not truncate.
        let expired = vec![
            serde_json::json!({"role": "user", "channel_media_pending_until": "1999-01-01T00:00:00Z"}),
        ];
        assert_eq!(trailing_user_messages(&expired, now).len(), 1);
    }
}
