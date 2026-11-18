//! Shared agent readiness verdict.
//!
//! Go keeps one function (`service.AgentReadiness`) that every admission path
//! calls, so the assign, mention and autopilot entries can never disagree about
//! whether a target is runnable. Rust had a partial copy inside the chat send
//! path (owner comparison only) and nothing else; this module is the single
//! source every Rust admission path now uses.
//!
//! The distinction that matters is not "ready or not" but whether waiting is a
//! plan. An offline machine comes back by itself, so its work queues. A target
//! bound to nothing, one whose runtime refuses its owner, or one whose CLI
//! cannot start will never pick that work up until a human acts — those are
//! refused with a reason code.
//!
//! The verdict carries more than the code on purpose. A refused trigger has no
//! response for the person who caused it to read (an agent-authored mention has
//! nobody watching at all), so the refusal leaves a durable notice on the issue
//! built from the daemon's own `Repair`/`Detail`. Keeping the notice text here,
//! beside the verdict that selects it, is what stops the two from drifting into
//! telling someone to reinstall a CLI that was never the problem.

use super::comment_dispatch::ReasonCode;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio_postgres::GenericClient;
use uuid::Uuid;

/// How long an `installing` claim on a runtime row is honoured.
///
/// The claim is what makes an offline runtime worth queueing behind, so a claim
/// that outlives its install recreates the failure the structured reason exists
/// to prevent. The daemon withdraws the claim when an install gives up, but a
/// withdraw cannot be guaranteed (shutdown, crash, restart mid-install), so the
/// claim is bounded rather than trusted. Same five minutes as Go.
const INSTALL_CLAIM_WINDOW_SECS: i64 = 300;

const OFFLINE_CODE_NOT_EXECUTABLE: &str = "not_executable";
const OFFLINE_CODE_DSH_PROFILE: &str = "dsh_profile";

/// What a readiness check concluded, in the vocabulary callers branch on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentAvailability {
    /// The agent can take work now.
    Available,
    /// Not runnable right now, but nothing is broken: queue the work.
    Waitable,
    /// Nothing will claim this agent's work until a human intervenes.
    Blocked,
}

/// The fix the daemon reported for an unusable runtime, mirroring its
/// `agent.ExecFormatRepair` wire shape. Absent for every other verdict, and for
/// a runtime whose daemon is too old to report one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeRepair {
    pub package: String,
    /// The command that reinstalls the broken entry point.
    pub command: String,
    /// The interpreter `command` is written for ("bash", "powershell"), used as
    /// the fence language so a Windows user is not handed POSIX syntax in a
    /// bash block. Empty from a daemon too old to report it.
    pub shell: String,
}

/// A readiness decision plus everything a caller needs to act on it.
#[derive(Clone, Debug)]
pub struct AgentVerdict {
    pub availability: AgentAvailability,
    /// Set for every non-available verdict; `None` only when available.
    pub reason: Option<ReasonCode>,
    pub repair: Option<RuntimeRepair>,
    /// The daemon's own description, for logs and for the durable notice.
    /// Never parsed.
    pub detail: String,
}

impl AgentVerdict {
    fn available() -> Self {
        Self {
            availability: AgentAvailability::Available,
            reason: None,
            repair: None,
            detail: String::new(),
        }
    }

    fn blocked(
        reason: ReasonCode,
        detail: impl Into<String>,
        repair: Option<RuntimeRepair>,
    ) -> Self {
        Self {
            availability: AgentAvailability::Blocked,
            reason: Some(reason),
            repair,
            detail: detail.into(),
        }
    }

    /// Waiting is futile: refuse the trigger rather than queue it.
    pub fn blocked_now(&self) -> bool {
        self.availability == AgentAvailability::Blocked
    }

    /// The reason code to report when refused. A blocked verdict always has
    /// one; the fallback is unreachable but must not invent a success code.
    pub fn blocked_reason(&self) -> Option<ReasonCode> {
        self.blocked_now()
            .then(|| self.reason.unwrap_or(ReasonCode::InternalError))
    }
}

/// The agent columns the verdict reads. Kept separate from a full row so the
/// routing decision can ask the question with facts it already holds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadinessInput {
    pub archived: bool,
    pub runtime_id: Option<Uuid>,
    /// `RuntimeAllowsUser` judges the *agent owner* against the machine's
    /// owner/share list, not the caller.
    pub owner_id: Option<Uuid>,
}

/// The daemon's structured explanation, stored on the runtime row's metadata by
/// the deregister handler. Unparseable metadata means "no explanation", never a
/// different verdict.
#[derive(Clone, Debug, Default, Deserialize)]
struct RuntimeOfflineReason {
    #[serde(default)]
    code: String,
    #[serde(default)]
    detail: String,
    #[serde(default)]
    repair: Option<RepairWire>,
    #[serde(default)]
    installing: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RepairWire {
    #[serde(default)]
    package: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    shell: String,
}

fn parse_offline_reason(metadata: Option<&serde_json::Value>) -> Option<RuntimeOfflineReason> {
    let raw = metadata?.get("offline_reason")?;
    // A null or non-object envelope has no explanation, exactly as Go's
    // `json.Unmarshal` into a pointer field yields nil.
    serde_json::from_value(raw.clone()).ok()
}

/// Report whether a blocked verdict's reason is one a human has to repair on the
/// runtime's machine, and therefore one that must leave a durable explanation on
/// the issue.
///
/// A predicate rather than an inline comparison because three admission paths
/// ask it — the refused mention, the refused assignment, the refused
/// assign-on-create — and a code added to one but not the others is a trigger
/// that vanishes with no trace on exactly the surfaces that have no response for
/// the user to read.
pub(crate) fn runtime_blocked_needs_notice(code: ReasonCode) -> bool {
    matches!(
        code,
        ReasonCode::RuntimeUnusable
            | ReasonCode::RuntimeProfileMissing
            | ReasonCode::RuntimeAccessDenied
    )
}

/// The durable explanation left on an issue when a trigger is refused because
/// the target's runtime cannot serve work and only a human on that machine can
/// change it.
///
/// The text is chosen by the verdict's reason, not by whether a repair command
/// happened to be present: an unrunnable CLI is reinstalled, while a CLI missing
/// its runtime profile is working perfectly and reinstalling it changes nothing.
pub(crate) fn runtime_unusable_notice(agent_name: &str, verdict: &AgentVerdict) -> String {
    let name = if agent_name.is_empty() {
        "The assigned agent"
    } else {
        agent_name
    };
    if verdict.reason == Some(ReasonCode::RuntimeAccessDenied) {
        return format!(
            "{name} cannot run on this private runtime, so this trigger was not queued. \
             Ask its owner to share the runtime with the agent owner or make it public, \
             or rebind/copy the agent to a runtime its owner can use."
        );
    }
    if verdict.reason == Some(ReasonCode::RuntimeProfileMissing) {
        return runtime_profile_missing_notice(name);
    }
    if let Some(repair) = verdict.repair.as_ref()
        && !repair.command.is_empty()
    {
        return format!(
            "{name} could not start: its CLI is installed but cannot be executed on that machine, \
             so this trigger was not queued.\n\n\
             Usually the package's postinstall was blocked (npm 12 allowScripts, pnpm 10 approve-builds, \
             `--ignore-scripts`, `--omit=optional`) and the bin entry is still a placeholder. \
             On that machine, run:\n\n\
             ```{}\n{}\n```\n\n\
             The runtime comes back on its own within a couple of minutes; trigger the agent again after that.",
            repair_fence_language(&repair.shell),
            repair.command,
        );
    }
    format!(
        "{name} could not start: its CLI is installed but cannot be executed on that machine, \
         so this trigger was not queued. Reinstall the agent CLI on that machine with install \
         scripts enabled; the runtime comes back on its own within a couple of minutes."
    )
}

/// Explains a runtime whose CLI runs but whose Multica runtime profile is absent
/// or needs manual migration.
///
/// No fenced command, deliberately: the install is
/// `dsh plugin --profile multica add <bundle>` and `<bundle>` is the operator's
/// own choice of package, directory or tarball, so a code block would present a
/// placeholder as something to paste and run.
fn runtime_profile_missing_notice(name: &str) -> String {
    format!(
        "{name} could not start: the DeepSeek Harness CLI is installed on that machine, but its \
         `multica` runtime profile is missing or needs migration, so this trigger was not queued.\n\n\
         Build a compatible `@multica-ai/dsh-runtime` bundle from \
         [multica-ai/dsh-multica-runtime](https://github.com/multica-ai/dsh-multica-runtime#local-development), \
         following its README's DSH version requirements. For a missing profile, install the built \
         directory or tarball with `dsh plugin --profile multica add`, or set \
         `MULTICA_DSH_PROFILE_BUNDLE` for the daemon to install it.\n\n\
         For an existing bridge configuration, back up the profile and install the compatible bundle \
         manually. In `dsh.profile.bundles`, replace `dsh-profile-multica` with `@multica-ai/dsh-runtime`; \
         if both are listed, remove `dsh-profile-multica`. If the first-party runtime is only in \
         `dependencies`, add it to `dsh.profile.bundles`. `MULTICA_DSH_PROFILE_BUNDLE` will not migrate \
         an existing bridge configuration.\n\n\
         The runtime registers on its own within a couple of minutes after that; trigger the agent \
         again then."
    )
}

/// Labels the code block with the shell the command was written for. A daemon
/// too old to report one predates the Windows rendering entirely, so its command
/// is POSIX by construction.
fn repair_fence_language(shell: &str) -> &'static str {
    if shell == "powershell" {
        "powershell"
    } else {
        "bash"
    }
}

/// Judge whether an agent can accept new work right now, and what the caller
/// should do when it cannot.
///
/// `Err(())` is a failed runtime lookup: Go returns an error there and every
/// caller treats it as "not blocked" rather than inventing a verdict.
pub(crate) async fn agent_readiness(
    db: &impl GenericClient,
    agent: ReadinessInput,
) -> Result<AgentVerdict, ()> {
    if agent.archived {
        return Ok(AgentVerdict::blocked(
            ReasonCode::TargetUnavailable,
            "agent is archived",
            None,
        ));
    }
    let Some(runtime_id) = agent.runtime_id else {
        return Ok(AgentVerdict::blocked(
            ReasonCode::AgentRuntimeRequired,
            "agent has no runtime bound",
            None,
        ));
    };
    let row = db
        .query_opt(
            "SELECT owner_id,visibility,shared_with,status,metadata,updated_at FROM agent_runtime WHERE id=$1",
            &[&runtime_id],
        )
        .await
        .map_err(|_| ())?
        .ok_or(())?;
    Ok(runtime_verdict(&row, agent.owner_id))
}

/// Combines runtime health with the ownership binding that determines whether
/// this agent can execute there.
fn runtime_verdict(row: &tokio_postgres::Row, owner_id: Option<Uuid>) -> AgentVerdict {
    let visibility: String = row.get("visibility");
    let runtime_owner: Option<Uuid> = row.get("owner_id");
    if visibility == "private" && runtime_owner.is_some() && !runtime_allows_user(row, owner_id) {
        return AgentVerdict::blocked(
            ReasonCode::RuntimeAccessDenied,
            "agent owner does not have access to the private runtime",
            None,
        );
    }
    let status: String = row.get("status");
    if status == "online" {
        return AgentVerdict::available();
    }
    let metadata: Option<serde_json::Value> = row.get("metadata");
    if let Some(reason) = parse_offline_reason(metadata.as_ref()) {
        let repair = reason.repair.clone().map(|wire| RuntimeRepair {
            package: wire.package,
            command: wire.command,
            shell: wire.shell,
        });
        match reason.code.as_str() {
            // The machine is reachable and its CLI cannot serve work: refuse
            // rather than queue, and carry the repair so the notice can show it.
            OFFLINE_CODE_NOT_EXECUTABLE => {
                return AgentVerdict::blocked(ReasonCode::RuntimeUnusable, reason.detail, repair);
            }
            // Same shape of finding, own code: the CLI is not the broken part,
            // so "reinstall the CLI" copy would send the user to the wrong
            // repair. An install the daemon is running right now is the
            // exception — that wait ends by itself, so the work queues.
            OFFLINE_CODE_DSH_PROFILE
                if !install_claim_holds(
                    &reason,
                    row.get::<_, Option<DateTime<Utc>>>("updated_at"),
                ) =>
            {
                return AgentVerdict::blocked(
                    ReasonCode::RuntimeProfileMissing,
                    reason.detail,
                    repair,
                );
            }
            _ => {}
        }
    }
    AgentVerdict {
        availability: AgentAvailability::Waitable,
        reason: Some(ReasonCode::RuntimeOffline),
        repair: None,
        detail: format!("agent runtime is {status}"),
    }
}

/// Machine-owner consent, evaluated after workspace membership. Sharing never
/// grants machine administration — it only lets a named human's agents run.
pub(super) fn runtime_allows_user(row: &tokio_postgres::Row, user_id: Option<Uuid>) -> bool {
    let Some(user_id) = user_id else {
        return false;
    };
    let Some(owner) = row.get::<_, Option<Uuid>>("owner_id") else {
        return false;
    };
    let visibility: String = row.get("visibility");
    let shared: Vec<Uuid> = row
        .get::<_, Option<Vec<Uuid>>>("shared_with")
        .unwrap_or_default();
    owner == user_id || visibility == "public" || shared.contains(&user_id)
}

/// A claim with no usable timestamp cannot be bounded, so it is not honoured:
/// refusing during a real install costs one retry the notice already asks for,
/// while honouring a stale claim queues work behind something that stopped.
fn install_claim_holds(reason: &RuntimeOfflineReason, updated_at: Option<DateTime<Utc>>) -> bool {
    if !reason.installing {
        return false;
    }
    let Some(updated_at) = updated_at else {
        return false;
    };
    Utc::now().signed_duration_since(updated_at).num_seconds() < INSTALL_CLAIM_WINDOW_SECS
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reason_of(value: serde_json::Value) -> RuntimeOfflineReason {
        parse_offline_reason(Some(&value)).expect("envelope parses")
    }

    fn verdict(reason: ReasonCode, repair: Option<RuntimeRepair>) -> AgentVerdict {
        AgentVerdict::blocked(reason, "", repair)
    }

    #[test]
    fn install_claim_needs_a_fresh_timestamp() {
        let reason =
            reason_of(json!({"offline_reason": {"code": "dsh_profile", "installing": true}}));
        assert!(install_claim_holds(&reason, Some(Utc::now())));
        assert!(!install_claim_holds(&reason, None));
        // A claim older than the window is read as a plain missing profile.
        assert!(!install_claim_holds(
            &reason,
            Some(Utc::now() - chrono::Duration::seconds(301))
        ));
        // A daemon that is not installing cannot satisfy the wait at all.
        let idle =
            reason_of(json!({"offline_reason": {"code": "dsh_profile", "installing": false}}));
        assert!(!install_claim_holds(&idle, Some(Utc::now())));
    }

    #[test]
    fn offline_reason_absent_or_malformed_is_no_explanation() {
        assert!(parse_offline_reason(None).is_none());
        assert!(parse_offline_reason(Some(&json!({}))).is_none());
        assert!(parse_offline_reason(Some(&json!({"offline_reason": null}))).is_none());
        assert!(parse_offline_reason(Some(&json!({"offline_reason": "x"}))).is_none());
    }

    /// The blocker set is exactly Go's: an offline runtime is queueable, so it
    /// never appears here.
    #[test]
    fn blocked_codes_are_the_repairable_ones() {
        for code in [
            ReasonCode::TargetUnavailable,
            ReasonCode::AgentRuntimeRequired,
            ReasonCode::RuntimeAccessDenied,
            ReasonCode::RuntimeUnusable,
            ReasonCode::RuntimeProfileMissing,
        ] {
            assert!(!code.is_success());
        }
        assert!(!ReasonCode::RuntimeOffline.is_success());
        assert!(runtime_blocked_needs_notice(ReasonCode::RuntimeUnusable));
        assert!(runtime_blocked_needs_notice(
            ReasonCode::RuntimeProfileMissing
        ));
        assert!(runtime_blocked_needs_notice(
            ReasonCode::RuntimeAccessDenied
        ));
        assert!(!runtime_blocked_needs_notice(ReasonCode::RuntimeOffline));
        assert!(!runtime_blocked_needs_notice(ReasonCode::TargetUnavailable));
    }

    /// Only a blocked verdict may be refused. A waitable one queues, so it must
    /// never produce a reason a caller could mistake for a refusal.
    #[test]
    fn only_blocked_verdicts_carry_a_refusal_reason() {
        let waitable = AgentVerdict {
            availability: AgentAvailability::Waitable,
            reason: Some(ReasonCode::RuntimeOffline),
            repair: None,
            detail: "agent runtime is offline".into(),
        };
        assert!(!waitable.blocked_now());
        assert_eq!(waitable.blocked_reason(), None);
        assert!(!AgentVerdict::available().blocked_now());
        assert_eq!(
            verdict(ReasonCode::RuntimeUnusable, None).blocked_reason(),
            Some(ReasonCode::RuntimeUnusable)
        );
    }

    #[test]
    fn notice_is_chosen_by_reason_not_by_repair_presence() {
        let access =
            runtime_unusable_notice("Builder", &verdict(ReasonCode::RuntimeAccessDenied, None));
        assert!(access.starts_with("Builder cannot run on this private runtime"));

        // Missing name falls back to Go's fixed subject.
        let unnamed = runtime_unusable_notice("", &verdict(ReasonCode::RuntimeAccessDenied, None));
        assert!(unnamed.starts_with("The assigned agent cannot run"));

        // A profile-missing verdict never shows a reinstall command, even when
        // the daemon reported one: the CLI is not the broken part.
        let profile = runtime_unusable_notice(
            "Builder",
            &verdict(
                ReasonCode::RuntimeProfileMissing,
                Some(RuntimeRepair {
                    package: "pkg".into(),
                    command: "npm i -g pkg".into(),
                    shell: "bash".into(),
                }),
            ),
        );
        assert!(profile.contains("its `multica` runtime profile is missing"));
        assert!(!profile.contains("npm i -g pkg"));
        assert!(!profile.contains("```"));
    }

    #[test]
    fn notice_renders_the_repair_in_the_machine_shell() {
        let bash = runtime_unusable_notice(
            "Builder",
            &verdict(
                ReasonCode::RuntimeUnusable,
                Some(RuntimeRepair {
                    package: "@anthropic-ai/claude-code".into(),
                    command: "npm i -g @anthropic-ai/claude-code".into(),
                    shell: String::new(),
                }),
            ),
        );
        assert!(bash.contains("```bash\nnpm i -g @anthropic-ai/claude-code\n```"));
        let powershell = runtime_unusable_notice(
            "Builder",
            &verdict(
                ReasonCode::RuntimeUnusable,
                Some(RuntimeRepair {
                    package: "pkg".into(),
                    command: "npm i -g pkg".into(),
                    shell: "powershell".into(),
                }),
            ),
        );
        assert!(powershell.contains("```powershell\nnpm i -g pkg\n```"));
        // A command-less repair keeps the useful text without an empty fence.
        let bare = runtime_unusable_notice(
            "Builder",
            &verdict(
                ReasonCode::RuntimeUnusable,
                Some(RuntimeRepair {
                    package: "pkg".into(),
                    command: String::new(),
                    shell: "bash".into(),
                }),
            ),
        );
        assert!(bare.contains("Reinstall the agent CLI on that machine"));
        assert!(!bare.contains("```"));
    }
}
