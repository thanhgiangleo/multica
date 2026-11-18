//! Who may invoke an agent, and with whose authority.
//!
//! Go splits this into `invokeAgentDecision` (the verdict) and `canInvokeAgent`
//! (the verdict plus an operator-facing log line). Both live here so every Rust
//! path — comment mentions, assignment, chat send — asks the same question.
//!
//! Three rules the shape protects:
//!
//! * only a member acts as themselves. An agent or system actor is trusted for
//!   the *human at the top of the chain*, never for its own id, so an
//!   unattributed agent comment can never borrow a stranger's grant;
//! * the owner branch comes first, before the permission mode is read, so an
//!   owner is never denied by a stale allow-list;
//! * a `workspace` target deliberately admits agent/system principals even with
//!   no resolved human. That exception is scoped to that one target type —
//!   member and team targets still require a matching human, so an unattributed
//!   trigger fails closed against someone's specific-people grant.

use tokio_postgres::GenericClient;
use uuid::Uuid;

/// The agent columns the verdict reads.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InvokeTarget<'a> {
    pub id: Uuid,
    pub owner_id: Option<Uuid>,
    pub permission_mode: &'a str,
}

/// Caller identity in the vocabulary the verdict branches on. `originator` is
/// the top-of-chain human for non-member actors; it is absent when no human
/// could be resolved.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Invoker<'a> {
    /// `member`, `agent` or `system`.
    pub actor_type: &'a str,
    pub actor_id: Uuid,
    pub originator: Option<Uuid>,
}

/// The pure verdict, with database reads already resolved into a workspace
/// membership flag and the agent's allow-list.
pub(crate) fn invoke_decision(
    target: InvokeTarget<'_>,
    invoker: Invoker<'_>,
    is_workspace_member: bool,
    invocation_targets: &[(String, Uuid)],
) -> bool {
    let effective_user = if invoker.actor_type == "member" {
        Some(invoker.actor_id)
    } else {
        invoker.originator
    };

    // The agent owner may always invoke their own agent, whatever the
    // allow-list says.
    if let Some(effective) = effective_user
        && target.owner_id == Some(effective)
    {
        return true;
    }

    // Private (or any unknown mode) is deny-by-default: no admin bypass, no
    // A2A bypass. Only the owner branch above passes.
    if target.permission_mode != "public_to" {
        return false;
    }

    // Agents and system triggers are workspace-internal principals: a workspace
    // target admits them even when no human originator resolved. Deliberate and
    // product-approved — webhook / system / workspace-wide automation must be
    // able to reach a `public_to workspace` agent.
    let workspace_broad = invoker.actor_type == "agent" || invoker.actor_type == "system";

    for (kind, target_id) in invocation_targets {
        match kind.as_str() {
            "workspace" if is_workspace_member || workspace_broad => return true,
            // Requires a resolved human; an agent/system trigger with no
            // originator never matches here and so fails closed.
            "member" => {
                if let Some(effective) = effective_user
                    && *target_id == effective
                {
                    return true;
                }
            }
            // Reserved: team membership does not exist in V1, so team targets
            // admit nobody.
            _ => {}
        }
    }
    false
}

/// Full gate: read the allow-list, read membership, decide.
///
/// A failed query denies rather than allows — a transient database error must
/// never widen access.
pub(crate) async fn can_invoke(
    db: &impl GenericClient,
    workspace_id: Uuid,
    target: InvokeTarget<'_>,
    invoker: Invoker<'_>,
) -> Result<bool, ()> {
    let effective_user = if invoker.actor_type == "member" {
        Some(invoker.actor_id)
    } else {
        invoker.originator
    };
    if let Some(effective) = effective_user
        && target.owner_id == Some(effective)
    {
        return Ok(true);
    }
    if target.permission_mode != "public_to" {
        return Ok(false);
    }
    let targets = db
        .query(
            "SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY target_type ASC, created_at ASC",
            &[&target.id],
        )
        .await
        .map_err(|_| ())?;
    let targets: Vec<(String, Uuid)> = targets
        .iter()
        .map(|row| (row.get("target_type"), row.get("target_id")))
        .collect();
    let is_workspace_member = if let Some(effective) = effective_user {
        db.query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace_id, &effective],
        )
        .await
        .map_err(|_| ())?
        .is_some()
    } else {
        false
    };
    Ok(invoke_decision(
        target,
        invoker,
        is_workspace_member,
        &targets,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGENT: Uuid = Uuid::from_u128(0xa1);
    const OWNER: Uuid = Uuid::from_u128(0xb1);
    const STRANGER: Uuid = Uuid::from_u128(0xc1);
    const TOP: Uuid = Uuid::from_u128(0xd1);

    fn target(mode: &'static str) -> InvokeTarget<'static> {
        InvokeTarget {
            id: AGENT,
            owner_id: Some(OWNER),
            permission_mode: mode,
        }
    }

    fn member(id: Uuid) -> Invoker<'static> {
        Invoker {
            actor_type: "member",
            actor_id: id,
            originator: None,
        }
    }

    fn agent(orig: Option<Uuid>) -> Invoker<'static> {
        Invoker {
            actor_type: "agent",
            actor_id: AGENT,
            originator: orig,
        }
    }

    #[test]
    fn owner_always_passes_even_when_private() {
        assert!(invoke_decision(
            target("private"),
            member(OWNER),
            false,
            &[]
        ));
        assert!(invoke_decision(
            target("private"),
            agent(Some(OWNER)),
            false,
            &[]
        ));
    }

    #[test]
    fn private_denies_non_owner_without_any_bypass() {
        assert!(!invoke_decision(
            target("private"),
            member(STRANGER),
            true,
            &[("workspace".into(), Uuid::nil())]
        ));
        // No admin/A2A escape hatch: even a workspace member is refused.
        assert!(!invoke_decision(
            target("private"),
            agent(Some(TOP)),
            true,
            &[]
        ));
    }

    #[test]
    fn public_workspace_target_admits_unattributed_agent_and_system() {
        let targets = [("workspace".to_string(), Uuid::nil())];
        assert!(invoke_decision(
            target("public_to"),
            agent(None),
            false,
            &targets
        ));
        let system = Invoker {
            actor_type: "system",
            actor_id: AGENT,
            originator: None,
        };
        assert!(invoke_decision(
            target("public_to"),
            system,
            false,
            &targets
        ));
    }

    #[test]
    fn public_member_target_requires_the_matching_human() {
        let targets = [("member".to_string(), TOP)];
        assert!(invoke_decision(
            target("public_to"),
            agent(Some(TOP)),
            false,
            &targets
        ));
        // An unattributed agent trigger fails closed rather than matching a
        // grant aimed at somebody specific.
        assert!(!invoke_decision(
            target("public_to"),
            agent(None),
            false,
            &targets
        ));
        assert!(!invoke_decision(
            target("public_to"),
            member(STRANGER),
            false,
            &targets
        ));
    }

    #[test]
    fn team_targets_admit_nobody() {
        let targets = [("team".to_string(), TOP)];
        assert!(!invoke_decision(
            target("public_to"),
            agent(Some(TOP)),
            true,
            &targets
        ));
        assert!(!invoke_decision(
            target("public_to"),
            member(TOP),
            true,
            &targets
        ));
    }

    #[test]
    fn a_member_never_becomes_its_own_originator() {
        // A member with an originator set still acts as themselves.
        let invoker = Invoker {
            actor_type: "member",
            actor_id: STRANGER,
            originator: Some(OWNER),
        };
        assert!(!invoke_decision(target("private"), invoker, false, &[]));
    }
}
