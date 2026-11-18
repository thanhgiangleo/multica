//! Pairing-secret interception and static private notices. No
//! transcript/context writes, tasks or notification fanout. Captions and media
//! remain unsupported by polling and must not bypass this gate when added.
use crate::api::channels_sprint_ingress::ClaimError;
use crate::api::channels_sprint_pair::{
    GuestPairError, VerifiedPrivateSender, redeem_guest_in_transaction,
};
use tokio_postgres::Transaction;

/// Safe post-commit reply directive, never a bearer code or guest identity.
/// Caller owns private-only delivery; Replay must not re-send a notice.
#[derive(Debug, PartialEq, Eq)]
pub enum PairingNotice {
    Paired,
    Invalid,
    Unavailable,
    Silent,
}
impl PairingNotice {
    pub fn text(&self) -> Option<&'static str> {
        match self {
            Self::Paired => Some(
                "You're paired. You can now message this agent here or mention it in a group. Group chat context is shared.",
            ),
            Self::Invalid => {
                Some("This invitation is invalid or expired. Ask the agent owner for a new link.")
            }
            Self::Unavailable => {
                Some("Pairing is temporarily unavailable. Please try again in a few minutes.")
            }
            Self::Silent => None,
        }
    }
}

/// Telegram has no sendMessage idempotency: attempted sends are never retried.
/// Pending directives committed with redemption may be recovered before a
/// provider attempt; an uncertain result cannot be retried automatically.
#[derive(Debug, PartialEq, Eq)]
pub enum PairingDelivery {
    Delivered,
    Refused,
    Unknown,
    NotSent,
    Denied,
    Silent,
    Replay,
}

pub(super) async fn send_notice(
    installer: &super::TelegramInstaller,
    token: &str,
    chat: i64,
    text: &'static str,
) -> PairingDelivery {
    use serde_json::{Value, json};
    let response = installer
        .http
        .post(format!("{}/bot{}/sendMessage", installer.origin, token))
        .timeout(std::time::Duration::from_secs(15))
        .header("content-type", "application/json")
        // Never quote the original message or send a reply anchor: it contains
        // a bearer code. No parse mode, markup, grant identity or user text.
        .body(json!({"chat_id":chat,"text":text}).to_string())
        .send()
        .await;
    let Ok(mut response) = response else {
        return PairingDelivery::Unknown;
    };
    let status = response.status();
    if status.is_client_error() {
        return PairingDelivery::Refused;
    }
    const MAX: usize = 65536;
    if !status.is_success() || response.content_length().is_some_and(|n| n > MAX as u64) {
        return PairingDelivery::Unknown;
    }
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if chunk.len() <= MAX - bytes.len() => bytes.extend_from_slice(&chunk),
            Ok(None) => break,
            _ => return PairingDelivery::Unknown,
        }
    }
    // Do not propagate provider descriptions, credential-bearing URLs or bodies.
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return PairingDelivery::Unknown;
    };
    if value["ok"] == false
        && value["error_code"]
            .as_u64()
            .is_some_and(|c| (400..500).contains(&c))
    {
        return PairingDelivery::Refused;
    }
    if value["ok"] == true
        && value["result"]["chat"]["id"].as_i64() == Some(chat)
        && value["result"]["message_id"]
            .as_i64()
            .is_some_and(|id| id > 0)
    {
        PairingDelivery::Delivered
    } else {
        PairingDelivery::Unknown
    }
}

pub(super) async fn redeem(
    tx: &Transaction<'_>,
    sender: VerifiedPrivateSender,
    code: String,
) -> Result<PairingNotice, ClaimError> {
    match redeem_guest_in_transaction(tx, &sender, &code).await {
        Ok(_) => Ok(PairingNotice::Paired),
        Err(
            GuestPairError::Invalid | GuestPairError::AlreadyPaired | GuestPairError::NotShared,
        ) => Ok(PairingNotice::Invalid),
        Err(GuestPairError::Throttled) => Ok(PairingNotice::Unavailable),
        // Unlike a product denial, an infrastructure failure may never commit
        // partial pairing or a processed marker. Retry stays connection-fenced.
        Err(GuestPairError::Database) => Err(ClaimError::Database),
    }
}

fn alphabet(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}
fn argument(word: &str) -> &str {
    word.trim_matches(|c: char| c != '_' && c != '-' && !c.is_alphanumeric())
}
fn command(word: &str) -> String {
    word.trim_matches(|c: char| c != '/' && !c.is_alphanumeric())
        .split('@')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}
fn code_like(word: &str) -> bool {
    word.len() >= 16 && word.bytes().all(alphabet)
}
fn deep_link_secret(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    let telegram = ["t.me/", "telegram.me/", "telegram.dog/", "tg://"]
        .iter()
        .any(|part| lower.contains(part));
    let mut offset = 0;
    while let Some(index) = lower[offset..].find("start=") {
        let index = offset + index;
        offset = index + 6;
        if index > 0
            && (lower.as_bytes()[index - 1].is_ascii_alphanumeric()
                || lower.as_bytes()[index - 1] == b'_')
        {
            continue;
        }
        let value = word[offset..].split(['&', '#']).next().unwrap_or("");
        let value = argument(value);
        if !value.is_empty()
            && !value.eq_ignore_ascii_case("pair")
            && (telegram || (value.len() == 43 && value.bytes().all(alphabet)))
        {
            return true;
        }
    }
    false
}

/// Mirrors the frozen Go secret gate, not a general command parser. Detects
/// malformed/embedded/other-bot attempts before normalization or persistence.
/// Bare /start, /pair and /start pair retain member-linking semantics.
pub(super) fn attempt(text: &str) -> bool {
    let words: Vec<_> = text.split_whitespace().collect();
    let mut leading = true;
    for (index, word) in words.iter().enumerate() {
        let at_leading = leading;
        if !word.starts_with('@') {
            leading = false;
        }
        let next = words.get(index + 1).map(|word| argument(word));
        match (command(word).as_str(), next) {
            ("/pair", Some(_)) => return true,
            ("/start", Some(arg)) => {
                if !arg.eq_ignore_ascii_case("pair") {
                    if at_leading || code_like(arg) {
                        return true;
                    }
                } else if let Some(arg) = words.get(index + 2)
                    && (at_leading || code_like(argument(arg)))
                {
                    return true;
                }
            }
            _ => {}
        }
        if deep_link_secret(word) {
            return true;
        }
    }
    false
}

fn normalize(text: &str, bot_username: &str) -> String {
    if bot_username.is_empty() {
        return text.trim().into();
    }
    let token = format!("@{}", bot_username.to_ascii_lowercase());
    let lower = text.to_ascii_lowercase();
    let mut start = 0;
    let mut out = String::new();
    while let Some(index) = lower[start..].find(&token) {
        let index = start + index;
        let end = index + token.len();
        if lower
            .as_bytes()
            .get(end)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            out.push_str(&text[start..end]);
        } else {
            out.push_str(&text[start..index]);
        }
        start = end;
    }
    out.push_str(&text[start..]);
    out.trim().into()
}

/// Only an exact two-field, case-sensitive command redeems. Its bot suffix is
/// stripped even for another bot, matching Go; embedded or malformed attempts
/// charge the sender budget with an empty code, never extract a pasted secret.
pub(super) fn redemption_code(text: &str, bot_username: &str) -> String {
    let normalized = normalize(text, bot_username);
    let words: Vec<_> = normalized.split_whitespace().collect();
    if words.len() != 2 || !matches!(words[0].split('@').next(), Some("/pair" | "/start")) {
        return String::new();
    }
    let code = words[1];
    if code.len() == 43 && code.bytes().all(alphabet) {
        code.into()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channels_sprint_pair_secret_interception_matrix() {
        let code = "A".repeat(43);
        for text in [
            format!("/pair {code}"),
            format!(" /start@other {code} "),
            format!("@Mine /pair {code}"),
            format!("please /pair {code}"),
            format!("please /start {code}"),
            format!("/Pair@other, {code}"),
            "/pair broken".into(),
            "/start broken".into(),
            "/start pair extra".into(),
            format!("https://t.me/Mine?start={code}"),
            "https://telegram.dog/Mine?start=broken".into(),
            "tg://resolve?domain=Mine&start=broken".into(),
            format!("https://example.invalid/?start={code}"),
            format!("‘/pair’ {code}"),
        ] {
            assert!(attempt(&text), "secret-bearing shape must be intercepted");
        }
        for text in [
            "",
            "/start",
            "/pair",
            "/start pair",
            "/start@Mine pair",
            "type /start to begin",
            "please /start pair later",
            "https://t.me/Mine?start=pair",
            "https://t.me/Mine?restart=broken",
            "https://t.me/Mine?mystart=broken",
            "https://t.me/Mine?start=",
            "https://example.invalid/?start=broken",
            "ordinary text",
            &code,
        ] {
            assert!(!attempt(text), "non-attempt must not redeem");
        }
    }
    #[test]
    fn channels_sprint_pair_exact_command_not_embedded_secret() {
        let code = "A".repeat(43);
        for text in [
            format!("/pair {code}"),
            format!("/start@Other {code}"),
            format!("@Mine /pair {code}"),
            format!("/start@Mine {code}"),
        ] {
            assert_eq!(redemption_code(&text, "Mine"), code);
        }
        for text in [
            format!("@MineExtra /pair {code}"),
            format!("@Other /pair {code}"),
            format!("please /pair {code}"),
            format!("/PAIR {code}"),
            format!("/start pair {code}"),
            format!("/pair {code} extra"),
            format!("/pair {code}="),
            format!("https://t.me/Mine?start={code}"),
            "/start pair".into(),
        ] {
            assert!(redemption_code(&text, "Mine").is_empty());
        }
    }
    #[test]
    fn channels_sprint_pair_notices_are_static_and_group_silent() {
        assert!(
            PairingNotice::Paired
                .text()
                .unwrap()
                .contains("Group chat context is shared.")
        );
        assert!(
            PairingNotice::Invalid
                .text()
                .unwrap()
                .contains("invalid or expired")
        );
        assert!(
            PairingNotice::Unavailable
                .text()
                .unwrap()
                .contains("try again")
        );
        assert_eq!(PairingNotice::Silent.text(), None);
    }
}
