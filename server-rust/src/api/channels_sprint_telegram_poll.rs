//! Opt-in Telegram text ingress, pairing notices and cancellable supervisor; no default runtime wiring.
//! Credentials and epoch are captured together before I/O, never per queued update.
use super::{TelegramInstaller, parse_token};
use crate::api::{
    channels_sprint::Channel,
    channels_sprint_ingress::{
        ClaimError, ClaimResult, InboundKey, InboundWork, claim_inbound, current_in_transaction,
        process_inbound, release_inbound,
    },
    channels_sprint_telegram::admission::{
        Admission, AdmittedText, PrivateText, persist_private_text, plain_text,
    },
};
use async_trait::async_trait;
use deadpool_postgres::{ClientWrapper, Pool};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_postgres::Transaction;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum PollError {
    InvalidInstallation,
    Database,
    ProviderUnavailable,
    CredentialsRejected,
    Conflict,
    RateLimited { retry_after_seconds: u64 },
    InvalidResponse,
    UnsupportedUpdate,
    Pending,
    PollerActive,
    LeaseLost,
}
impl From<ClaimError> for PollError {
    fn from(error: ClaimError) -> Self {
        match error {
            ClaimError::Invalid => Self::InvalidInstallation,
            ClaimError::Database => Self::Database,
        }
    }
}

/// Constructible only from an authenticated Bot API response. No Debug/Serialize:
/// message text may contain private pairing material. No ambient user identity.
pub struct TelegramTextUpdate {
    update_id: i64,
    message_id: i64,
    chat_id: i64,
    sender_id: i64,
    thread_id: Option<i64>,
    private: bool,
    text: String,
    display_name: String,
    // Prevent passing a queued update from another installation or connection.
    connection: Uuid,
}
impl TelegramTextUpdate {
    pub fn update_id(&self) -> i64 {
        self.update_id
    }
    pub fn chat_id(&self) -> i64 {
        self.chat_id
    }
    pub fn sender_id(&self) -> i64 {
        self.sender_id
    }
    pub fn message_id(&self) -> i64 {
        self.message_id
    }
    pub fn thread_id(&self) -> Option<i64> {
        self.thread_id
    }
    pub fn is_private(&self) -> bool {
        self.private
    }
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn is_pairing_attempt(&self) -> bool {
        super::pairing::attempt(&self.text)
    }
}

/// A single runtime-owned polling connection. Do not share tokens or reload epochs.
/// Restart starts at offset zero; durable dedup absorbs completed replay.
/// The detached PostgreSQL connection holds a cross-replica advisory lease.
/// Dropping the session closes it rather than recycling a locked connection.
pub struct TelegramPollSession<'a> {
    installer: &'a TelegramInstaller,
    lease: ClientWrapper,
    pool: Arc<Pool>,
    key: InboundKey,
    token: String,
    bot_username: String,
    connection: Uuid,
    offset: i64,
    pending: VecDeque<i64>,
}
impl TelegramInstaller {
    pub async fn open_poll_session(
        &self,
        pool: Arc<Pool>,
        workspace: Uuid,
        installation: Uuid,
    ) -> Result<TelegramPollSession<'_>, PollError> {
        const INSTALLATION: &str = "SELECT i.config FROM channel_installation i JOIN agent a ON a.id=i.agent_id AND a.workspace_id=i.workspace_id AND a.archived_at IS NULL JOIN workspace w ON w.id=i.workspace_id WHERE i.id=$1 AND i.workspace_id=$2 AND i.channel_type='telegram' AND i.status='active'";
        // Check the workspace before contending for a lease. Otherwise a caller
        // from another workspace could distinguish a live poller from an invalid
        // installation. Recheck after acquiring the lock to fence revocation.
        let db = pool.get().await.map_err(|_| PollError::Database)?;
        if db
            .query_opt(INSTALLATION, &[&installation, &workspace])
            .await
            .map_err(|_| PollError::Database)?
            .is_none()
        {
            return Err(PollError::InvalidInstallation);
        }
        // Session-level locks are safe only on a detached client: dropping a
        // cancelled future aborts its connection and releases the lock in PG.
        // Never return a locked connection to the pool, even on errors.
        let lease = deadpool_postgres::Client::take(db);
        let acquired: bool = lease
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1::uuid::text, 73473))",
                &[&installation],
            )
            .await
            .map_err(|_| PollError::Database)?
            .get(0);
        if !acquired {
            return Err(PollError::PollerActive);
        }
        let row = lease
            .query_opt(INSTALLATION, &[&installation, &workspace])
            .await
            .map_err(|_| PollError::Database)?
            .ok_or(PollError::InvalidInstallation)?;
        let config: Value = row.get(0);
        let raw_generation = config["generation"]
            .as_str()
            .ok_or(PollError::InvalidInstallation)?;
        let generation =
            Uuid::parse_str(raw_generation).map_err(|_| PollError::InvalidInstallation)?;
        if generation.to_string() != raw_generation {
            return Err(PollError::InvalidInstallation);
        }
        let encrypted = config["bot_token_encrypted"]
            .as_str()
            .ok_or(PollError::InvalidInstallation)?;
        let token = String::from_utf8(
            self.cipher
                .open(encrypted)
                .map_err(|_| PollError::InvalidInstallation)?,
        )
        .map_err(|_| PollError::InvalidInstallation)?;
        let bot_id = parse_token(&token).map_err(|_| PollError::InvalidInstallation)?;
        if config["app_id"].as_str() != Some(bot_id) {
            return Err(PollError::InvalidInstallation);
        }
        Ok(TelegramPollSession {
            installer: self,
            lease,
            pool,
            key: InboundKey {
                workspace,
                installation,
                channel: Channel::Telegram,
                message_id: String::new(),
                generation: Some(generation),
            },
            token,
            bot_username: config["bot_username"]
                .as_str()
                .filter(|name| {
                    name.len() <= 64 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
                .unwrap_or("")
                .into(),
            connection: Uuid::new_v4(),
            offset: 0,
            pending: VecDeque::new(),
        })
    }
}
impl TelegramPollSession<'_> {
    pub fn offset(&self) -> i64 {
        self.offset
    }

    async fn current(&self) -> Result<(), PollError> {
        // Losing the lease connection during external I/O must fail closed even
        // if a fresh pooled connection can still read the installation epoch.
        self.lease
            .query_one("SELECT 1", &[])
            .await
            .map_err(|_| PollError::LeaseLost)?;
        let mut db = self.pool.get().await.map_err(|_| PollError::Database)?;
        let tx = db.transaction().await.map_err(|_| PollError::Database)?;
        if !current_in_transaction(&tx, &self.key).await? {
            return Err(PollError::InvalidInstallation);
        }
        tx.commit().await.map_err(|_| PollError::Database)
    }

    /// One bounded long poll, 50s hold/65s HTTP deadline. No retry sleeps or tasks.
    /// Caller owns cancellation by dropping this future; offset is never changed
    /// here. A stale response cannot be relabelled with a newly installed epoch.
    pub async fn get_updates(&mut self) -> Result<Vec<TelegramTextUpdate>, PollError> {
        if !self.pending.is_empty() {
            return Err(PollError::Pending);
        }
        self.current().await?;
        let mut response = self.installer.http
            .post(format!("{}/bot{}/getUpdates", self.installer.origin, self.token))
            .timeout(Duration::from_secs(65))
            .header("content-type", "application/json")
            .body(json!({"offset":self.offset,"timeout":50,"limit":100,"allowed_updates":["message"]}).to_string())
            .send().await.map_err(|_| PollError::ProviderUnavailable)?;
        let status = response.status();
        const MAX: usize = 1024 * 1024;
        if response.content_length().is_some_and(|n| n > MAX as u64) {
            return Err(PollError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PollError::ProviderUnavailable)?
        {
            if chunk.len() > MAX - bytes.len() {
                return Err(PollError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        let envelope: Value =
            serde_json::from_slice(&bytes).map_err(|_| PollError::InvalidResponse)?;
        // A dead lease takes precedence over an HTTP error (including 409):
        // the supervisor must abandon this session and reacquire before retry.
        self.current().await?;
        if !status.is_success() || envelope["ok"] != true {
            // Never propagate descriptions, transport errors or credential-bearing URLs.
            return Err(
                match envelope["error_code"]
                    .as_u64()
                    .unwrap_or(status.as_u16() as u64)
                {
                    401 => PollError::CredentialsRejected,
                    409 => PollError::Conflict,
                    429 => PollError::RateLimited {
                        retry_after_seconds: envelope["parameters"]["retry_after"]
                            .as_u64()
                            .unwrap_or(1)
                            .max(1),
                    },
                    _ => PollError::ProviderUnavailable,
                },
            );
        }
        let updates = envelope["result"]
            .as_array()
            .filter(|a| a.len() <= 100)
            .ok_or(PollError::InvalidResponse)?;
        let mut previous = self.offset - 1;
        let mut result = Vec::new();
        for update in updates {
            let item = parse_update(update, self.connection)?;
            if item.update_id <= previous {
                return Err(PollError::InvalidResponse);
            }
            previous = item.update_id;
            result.push(item);
        }
        self.pending = result.iter().map(|u| u.update_id).collect();
        Ok(result)
    }

    /// Intercept a pairing attempt before text admission. Only an authenticated
    /// private human may redeem; group attempts commit a silent numeric marker.
    /// Invitation/grant/shadow writes, sender budget and processed marker commit
    /// together. Reply directives are safe static text for post-commit delivery;
    /// this method sends nothing. Infrastructure failures leave the update pending.
    /// Bare member-link commands and all other commands remain unsupported.
    pub async fn process_pairing(
        &mut self,
        update: &TelegramTextUpdate,
    ) -> Result<PollCommit<super::pairing::PairingNotice>, PollError> {
        if !update.is_pairing_attempt() {
            return Err(PollError::UnsupportedUpdate);
        }
        let private = update.private && update.thread_id.is_none();
        let sender = crate::api::channels_sprint_pair::VerifiedPrivateSender {
            channel: Channel::Telegram,
            installation: self.key.installation,
            external_user_id: update.sender_id.to_string(),
            display_name: update.display_name.clone(),
        };
        let code = super::pairing::redemption_code(&update.text, &self.bot_username);
        let key = self.key.clone();
        let update_message = format!("{}:{}", update.chat_id, update.message_id);
        let update_chat = update.chat_id;
        self.process_update(update, move |tx| {
            Box::pin(async move {
                if !private {
                    return Ok(super::pairing::PairingNotice::Silent);
                }
                let notice = super::pairing::redeem(tx, sender, code).await?;
                super::notice::record(tx, &key, update_message, update_chat, &notice).await?;
                Ok(notice)
            })
        })
        .await
    }

    /// Commit redemption, processed marker and a static notice directive together.
    /// The first delivery attempt is claimed durably before HTTP; a lost response
    /// is not retried because Telegram has no sendMessage idempotency key.
    pub async fn process_pairing_and_reply(
        &mut self,
        update: &TelegramTextUpdate,
    ) -> Result<super::pairing::PairingDelivery, PollError> {
        use super::pairing::PairingDelivery;
        match self.process_pairing(update).await? {
            PollCommit::Replay => Ok(PairingDelivery::Replay),
            PollCommit::Committed(super::pairing::PairingNotice::Silent) => {
                Ok(PairingDelivery::Silent)
            }
            PollCommit::Committed(_) => {
                let message = format!("{}:{}", update.chat_id, update.message_id);
                self.deliver_notice(&message).await
            }
        }
    }

    async fn deliver_notice(
        &self,
        message: &str,
    ) -> Result<super::pairing::PairingDelivery, PollError> {
        super::notice::deliver(
            self.installer,
            &self.pool,
            &self.lease,
            &self.key,
            &self.token,
            message,
        )
        .await
    }

    /// Recover only notices not yet attempted. A crash after the claim but
    /// before/during HTTP leaves an indeterminate attempt, never a resend.
    pub async fn recover_pairing_notices(&self) -> Result<usize, PollError> {
        self.current().await?;
        let db = self.pool.get().await.map_err(|_| PollError::Database)?;
        let generation = self.key.generation.ok_or(PollError::InvalidInstallation)?;
        let rows = db.query(
            "SELECT message_id FROM channel_pairing_notice WHERE workspace_id=$1 AND installation_id=$2 AND generation=$3 AND status='pending' ORDER BY created_at LIMIT 20",
            &[&self.key.workspace, &self.key.installation, &generation],
        ).await.map_err(|_| PollError::Database)?;
        for row in &rows {
            let message: String = row.get(0);
            self.deliver_notice(&message).await?;
        }
        Ok(rows.len())
    }

    /// Admit a conservative private plain-text turn using only DB-resolved
    /// member/guest identity. Session, message, context snapshot and processed
    /// marker commit together. Product denials commit Dropped without content.
    /// Unsupported commands/groups/secrets stay pending, never acknowledged.
    /// Task enqueue/recovery and event publication remain runtime responsibilities.
    pub async fn process_private_text(
        &mut self,
        update: &TelegramTextUpdate,
    ) -> Result<PollCommit<Admission>, PollError> {
        if !update.private || update.thread_id.is_some() || !plain_text(&update.text) {
            return Err(PollError::UnsupportedUpdate);
        }
        let input = PrivateText {
            chat_id: update.chat_id,
            sender_id: update.sender_id,
            message_id: update.message_id,
            text: update.text.clone(),
        };
        let key = self.key.clone();
        self.process_update(update, move |tx| {
            Box::pin(async move { persist_private_text(tx, &key, input).await })
        })
        .await
    }

    /// Supervisor admission must atomically enqueue via the shared scheduler.
    /// No default implementation: storing a message without a durable task must
    /// not acknowledge a turn. Callback failure rolls admission and dedup back.
    pub async fn process_private_text_with_enqueue(
        &mut self,
        update: &TelegramTextUpdate,
        enqueue: Arc<dyn PrivateTaskEnqueue>,
    ) -> Result<PollCommit<Admission>, PollError> {
        if !update.private || update.thread_id.is_some() || !plain_text(&update.text) {
            return Err(PollError::UnsupportedUpdate);
        }
        let input = PrivateText {
            chat_id: update.chat_id,
            sender_id: update.sender_id,
            message_id: update.message_id,
            text: update.text.clone(),
        };
        let key = self.key.clone();
        self.process_update(update, move |tx| {
            Box::pin(async move {
                let result = persist_private_text(tx, &key, input).await?;
                if let Admission::Persisted(admitted) = &result {
                    let task = enqueue.enqueue(tx, admitted).await?;
                    let matches = tx.query_opt(
                        "SELECT 1 FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND chat_session_id=$3 AND initiator_user_id=$4 AND channel_context_revision=$5 AND status='queued' AND retry_of_task_id IS NULL FOR SHARE",
                        &[&task, &admitted.agent, &admitted.session, &admitted.sender, &admitted.context_revision],
                    ).await.map_err(|_| ClaimError::Database)?.is_some();
                    if !matches {
                        return Err(ClaimError::Invalid);
                    }
                    super::reply::snapshot_private_task(tx, task).await.map_err(|error| match error {
                        super::reply::ReplyError::Database => ClaimError::Database,
                        _ => ClaimError::Invalid,
                    })?;
                }
                Ok(result)
            })
        })
        .await
    }

    /// Persist text admission and processed marker in ONE transaction. Callback
    /// must enforce bindings/guest grant/workspace/task policy before writing and
    /// must not perform external I/O. Publish/dispatch only after Committed.
    /// Busy claims never acknowledge Telegram; offset advances only after durable
    /// success or a confirmed processed replay. An error leaves offset unchanged.
    pub async fn process_update<T, F>(
        &mut self,
        update: &TelegramTextUpdate,
        persist: F,
    ) -> Result<PollCommit<T>, PollError>
    where
        F: for<'a, 'b> FnOnce(&'a Transaction<'b>) -> InboundWork<'a, T>,
    {
        if update.connection != self.connection || self.pending.front() != Some(&update.update_id) {
            return Err(PollError::InvalidResponse);
        }
        self.lease
            .query_one("SELECT 1", &[])
            .await
            .map_err(|_| PollError::LeaseLost)?;
        let key = InboundKey {
            // Frozen Go uses chat_id:message_id, not update_id, for message dedup.
            message_id: format!("{}:{}", update.chat_id, update.message_id),
            ..self.key.clone()
        };
        let commit = match claim_inbound(&self.pool, key.clone()).await? {
            ClaimResult::Acquired(claim) => {
                match process_inbound(&self.pool, &claim, persist).await {
                    Ok(Some(value)) => PollCommit::Committed(value),
                    Ok(None) => return Err(PollError::InvalidInstallation),
                    Err(error) => {
                        // Callback writes have rolled back. Never release somebody else's claim.
                        let _ = release_inbound(&self.pool, &claim).await;
                        return Err(error.into());
                    }
                }
            }
            ClaimResult::Duplicate => {
                let mut db = self.pool.get().await.map_err(|_| PollError::Database)?;
                let tx = db.transaction().await.map_err(|_| PollError::Database)?;
                if !current_in_transaction(&tx, &key).await? {
                    return Err(PollError::InvalidInstallation);
                }
                let processed = tx.query_opt("SELECT 1 FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id=$2 AND processed_at IS NOT NULL", &[&key.installation, &key.message_id]).await.map_err(|_| PollError::Database)?.is_some();
                tx.commit().await.map_err(|_| PollError::Database)?;
                if !processed {
                    return Err(PollError::Pending);
                }
                PollCommit::Replay
            }
        };
        self.offset = update.update_id + 1;
        self.pending.pop_front();
        Ok(commit)
    }
}

/// Runtime-owned durable enqueue hook. Called only for admitted member/guest
/// messages, inside the same DB transaction as message, context and dedup.
/// Implementations must select an authorized runtime and create a queued task,
/// returning its ID. The adapter validates its identity/context and snapshots
/// delivery before commit. Never perform network I/O or commit separately here.
#[async_trait]
pub trait PrivateTaskEnqueue: Send + Sync {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        admitted: &AdmittedText,
    ) -> Result<Uuid, ClaimError>;
}

/// Run one explicitly configured installation until cancelled or a terminal
/// error. The poll session holds a detached PostgreSQL advisory lease, so
/// direct pollers and other supervisors cannot run alongside it. A 409 from an unrelated external
/// poller is never hidden by retry. No global poller is started by this module.
/// Private pairing attempts are intercepted first and get a durable static
/// notice directive; an attempted send is never retried. Other supported text requires
/// atomic enqueue. Bare member-link commands and media remain unsupported.
pub async fn supervise_private_text(
    installer: &TelegramInstaller,
    pool: Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
    mut stop: watch::Receiver<bool>,
    enqueue: Arc<dyn PrivateTaskEnqueue>,
) -> Result<(), PollError> {
    let mut session = tokio::select! {
        biased;
        _ = stopped(&mut stop) => return Ok(()),
        opened = installer.open_poll_session(pool.clone(), workspace, installation) => opened?,
    };
    let generation = session.key.generation;
    let mut retry = Duration::from_secs(2);
    'poll: loop {
        let recovery = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return Ok(()),
            recovered = session.recover_pairing_notices() => recovered,
        };
        match recovery {
            Ok(_) => {}
            Err(PollError::LeaseLost) => {
                drop(session);
                if !wait_or_stop(&mut stop, retry).await {
                    return Ok(());
                }
                retry = retry.saturating_mul(2).min(Duration::from_secs(60));
                session = tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => return Ok(()),
                    opened = installer.open_poll_session(pool.clone(), workspace, installation) => opened?,
                };
                if session.key.generation != generation {
                    return Err(PollError::InvalidInstallation);
                }
                continue 'poll;
            }
            Err(PollError::Database) => {
                if !wait_or_stop(&mut stop, retry).await {
                    return Ok(());
                }
                retry = retry.saturating_mul(2).min(Duration::from_secs(60));
                continue 'poll;
            }
            Err(error) => return Err(error),
        }
        let updates = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return Ok(()),
            fetched = session.get_updates() => fetched,
        };
        let updates = match updates {
            Ok(updates) => {
                retry = Duration::from_secs(2);
                updates
            }
            Err(PollError::LeaseLost) => {
                // Discard this session (and its unacknowledged batch) before
                // reacquiring. A new connection must retain the original epoch.
                drop(session);
                if !wait_or_stop(&mut stop, retry).await {
                    return Ok(());
                }
                retry = retry.saturating_mul(2).min(Duration::from_secs(60));
                session = tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => return Ok(()),
                    opened = installer.open_poll_session(pool.clone(), workspace, installation) => opened?,
                };
                if session.key.generation != generation {
                    return Err(PollError::InvalidInstallation);
                }
                continue 'poll;
            }
            Err(
                error @ (PollError::Database | PollError::ProviderUnavailable | PollError::Pending),
            ) => {
                let wait = if error == PollError::Pending {
                    Duration::from_millis(250)
                } else {
                    retry
                };
                if error != PollError::Pending {
                    retry = retry.saturating_mul(2).min(Duration::from_secs(60));
                }
                if wait_or_stop(&mut stop, wait).await {
                    continue;
                }
                return Ok(());
            }
            Err(PollError::RateLimited {
                retry_after_seconds,
            }) => {
                if retry_after_seconds > 86_400 {
                    return Err(PollError::RateLimited {
                        retry_after_seconds,
                    });
                }
                if wait_or_stop(&mut stop, Duration::from_secs(retry_after_seconds.max(1))).await {
                    continue;
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if updates.is_empty() && !wait_or_stop(&mut stop, Duration::from_millis(250)).await {
            return Ok(());
        }
        for update in &updates {
            let mut retry = Duration::from_millis(250);
            loop {
                let result = tokio::select! {
                    _ = stopped(&mut stop) => return Ok(()),
                    processed = async {
                        if update.is_pairing_attempt() {
                            // Always intercept bearer commands before admission.
                            // Send failures consume only this pairing update,
                            // never poison the rest of the installation's batch.
                            session.process_pairing_and_reply(update).await.map(|_| ())
                        } else {
                            session.process_private_text_with_enqueue(update, enqueue.clone()).await.map(|_| ())
                        }
                    } => processed,
                };
                match result {
                    Ok(()) => break,
                    Err(PollError::LeaseLost) => {
                        drop(session);
                        if !wait_or_stop(&mut stop, retry).await {
                            return Ok(());
                        }
                        session = tokio::select! {
                            biased;
                            _ = stopped(&mut stop) => return Ok(()),
                            opened = installer.open_poll_session(pool.clone(), workspace, installation) => opened?,
                        };
                        if session.key.generation != generation {
                            return Err(PollError::InvalidInstallation);
                        }
                        continue 'poll;
                    }
                    Err(PollError::Pending | PollError::Database) => {
                        if !wait_or_stop(&mut stop, retry).await {
                            return Ok(());
                        }
                        retry = retry.saturating_mul(2).min(Duration::from_secs(30));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() && stop.changed().await.is_ok() {}
}
async fn wait_or_stop(stop: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        _ = stopped(stop) => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PollCommit<T> {
    Committed(T),
    Replay,
}

fn parse_update(value: &Value, connection: Uuid) -> Result<TelegramTextUpdate, PollError> {
    let update_id = value["update_id"]
        .as_i64()
        .filter(|n| *n >= 0 && *n < i64::MAX)
        .ok_or(PollError::InvalidResponse)?;
    let msg = value.get("message").ok_or(PollError::UnsupportedUpdate)?;
    let message_id = msg["message_id"]
        .as_i64()
        .filter(|n| *n > 0)
        .ok_or(PollError::InvalidResponse)?;
    let sender_id = msg["from"]["id"]
        .as_i64()
        .filter(|n| *n > 0)
        .ok_or(PollError::UnsupportedUpdate)?;
    if msg["from"]["is_bot"] != false {
        return Err(PollError::UnsupportedUpdate);
    }
    let chat_id = msg["chat"]["id"]
        .as_i64()
        .filter(|n| *n != 0)
        .ok_or(PollError::InvalidResponse)?;
    let private = match msg["chat"]["type"].as_str() {
        Some("private") if chat_id == sender_id => true,
        Some("group" | "supergroup") if chat_id < 0 => false,
        _ => return Err(PollError::UnsupportedUpdate),
    };
    let text = msg["text"]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 65536)
        .ok_or(PollError::UnsupportedUpdate)?
        .to_owned();
    let thread_id = match msg.get("message_thread_id") {
        None => None,
        Some(v) => Some(
            v.as_i64()
                .filter(|n| *n > 0)
                .ok_or(PollError::InvalidResponse)?,
        ),
    };
    let display_name = format!(
        "{} {}",
        msg["from"]["first_name"].as_str().unwrap_or(""),
        msg["from"]["last_name"].as_str().unwrap_or("")
    );
    let display_name: String = display_name.trim().chars().take(1024).collect();
    Ok(TelegramTextUpdate {
        update_id,
        message_id,
        chat_id,
        sender_id,
        thread_id,
        private,
        text,
        display_name,
        connection,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channels_sprint_telegram_poll_parser_fails_closed() {
        let conn = Uuid::new_v4();
        let valid = json!({"update_id":0,"message":{"message_id":1,"from":{"id":123,"is_bot":false},"chat":{"id":123,"type":"private"},"text":"synthetic"}});
        assert_eq!(parse_update(&valid, conn).unwrap().chat_id(), 123);
        for bad in [
            json!({"update_id":i64::MAX}),
            json!({"update_id":-1}),
            json!({"update_id":0,"edited_message":valid["message"]}),
        ] {
            assert!(parse_update(&bad, conn).is_err());
        }
        for (pointer, replacement) in [
            ("/message/from/is_bot", json!(true)),
            ("/message/from/id", json!(0)),
            ("/message/chat/id", json!(456)),
            ("/message/text", json!("")),
            ("/message/message_id", json!(0)),
            ("/message/chat/type", json!("channel")),
        ] {
            let mut bad = valid.clone();
            *bad.pointer_mut(pointer).unwrap() = replacement;
            assert!(parse_update(&bad, conn).is_err());
        }
    }
}
