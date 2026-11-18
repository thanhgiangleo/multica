//! Secret and PostgreSQL text sanitization for daemon-reported task messages.
//! Keep this before the batch insert AND before publishing the same rows to WS.
use regex::Regex;
use serde_json::{Map, Value};
use std::{collections::HashSet, sync::OnceLock};

const DEPTH_LIMIT: usize = 32;
const DEPTH_REPLACEMENT: &str = "[REDACTED DEPTH LIMIT]";

fn patterns() -> &'static [(Regex, &'static str)] {
    static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            (r"(?-u:\b)AKIA[0-9A-Z]{16}(?-u:\b)", "[REDACTED AWS KEY]"),
            (r"(?i)(?:aws_secret_access_key|secret_?access_?key)[\t\n\f\r ]*[=:][\t\n\f\r ]*[A-Za-z0-9/+=]{40}", "[REDACTED AWS SECRET]"),
            (r"(?s)-----BEGIN[A-Z\t\n\f\r ]*PRIVATE KEY-----.*?-----END[A-Z\t\n\f\r ]*PRIVATE KEY-----", "[REDACTED PRIVATE KEY]"),
            (r"(?-u:\b)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{36,255}(?-u:\b)", "[REDACTED GITHUB TOKEN]"),
            (r"(?-u:\b)github_pat_[A-Za-z0-9_]{20,255}(?-u:\b)", "[REDACTED GITHUB TOKEN]"),
            (r"(?-u:\b)sk-[A-Za-z0-9_-]{20,}(?-u:\b)", "[REDACTED API KEY]"),
            (r"(?-u:\b)xox[bporase]-[A-Za-z0-9-]{10,}(?-u:\b)", "[REDACTED SLACK TOKEN]"),
            (r"(?-u:\b)xapp-[A-Za-z0-9-]{10,}(?-u:\b)", "[REDACTED SLACK TOKEN]"),
            (r"(?-u:\b)glpat-[A-Za-z0-9_-]{20,}(?-u:\b)", "[REDACTED GITLAB TOKEN]"),
            (r"(?-u:\b)AIza[0-9A-Za-z_-]{35}([^0-9A-Za-z_-]|$)", "[REDACTED GOOGLE API KEY]$1"),
            (r"(?-u:\b)(?:sk|rk)_live_[0-9A-Za-z]{16,}(?-u:\b)", "[REDACTED STRIPE KEY]"),
            (r"(?-u:\b)ey[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}(?-u:\b)", "[REDACTED JWT]"),
            (r"(?i)(?-u:\b)Bearer[\t\n\f\r ]+[A-Za-z0-9\-._~+/]+=*(?-u:\b)", "Bearer [REDACTED]"),
            (r"(?i)(?:postgres|mysql|mongodb|redis|amqp)(?:ql)?://[^:\t\n\f\r ]+:[^@\t\n\f\r ]+@", "[REDACTED CONNECTION STRING]@"),
            (r"(?i)(?:API_KEY|API_SECRET|SECRET_KEY|SECRET|ACCESS_TOKEN|AUTH_TOKEN|PRIVATE_KEY|DATABASE_URL|DB_PASSWORD|DB_URL|REDIS_URL|PASSWORD|TOKEN)[\t\n\f\r ]*[=:][\t\n\f\r ]*[^\t\n\f\r ]+", "[REDACTED CREDENTIAL]"),
        ]
        .into_iter()
        .map(|(pattern, replacement)| (Regex::new(pattern).expect("static secret regex"), replacement))
        .collect()
    })
}

pub(super) fn text(raw: &str) -> String {
    let mut value = raw.to_owned();
    for (pattern, replacement) in patterns() {
        value = pattern.replace_all(&value, *replacement).into_owned();
    }
    value.replace('\0', "")
}

fn sanitized_map(map: &Map<String, Value>, depth: usize) -> Map<String, Value> {
    let mut safe = Map::new();
    let mut used = HashSet::new();
    let mut changed = Vec::new();
    // Reserve unchanged keys first. A NUL-stripped key cannot overwrite an existing key.
    for (key, value) in map {
        let clean = key.replace('\0', "");
        if clean == *key {
            used.insert(clean.clone());
            safe.insert(clean, input(value, depth + 1));
        } else {
            changed.push((key, clean, value));
        }
    }
    for (_, clean, value) in changed {
        let mut key = clean.clone();
        let mut suffix = 2;
        while !used.insert(key.clone()) {
            key = format!("{clean}#{suffix}");
            suffix += 1;
        }
        safe.insert(key, input(value, depth + 1));
    }
    safe
}

fn input(value: &Value, depth: usize) -> Value {
    if depth >= DEPTH_LIMIT {
        return Value::String(DEPTH_REPLACEMENT.into());
    }
    match value {
        Value::String(value) => Value::String(text(value)),
        Value::Array(values) => Value::Array(values.iter().map(|v| input(v, depth + 1)).collect()),
        Value::Object(values) => Value::Object(sanitized_map(values, depth)),
        _ => value.clone(),
    }
}

pub(super) fn input_map(value: &Value) -> Value {
    // Go's InputMap begins with depth 0; nested keys and values are scrubbed.
    input(value, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn go_ascii_whitespace_and_depth_limit_are_preserved() {
        assert_eq!(text("DB_PASSWORD: hunter2"), "[REDACTED CREDENTIAL]");
        assert_eq!(
            text("connecting postgres://user:pass\u{a0}word@host"),
            "connecting [REDACTED CONNECTION STRING]@host"
        );
        let pem = "-----BEGIN\u{a0}PRIVATE KEY-----data-----END\u{a0}PRIVATE KEY-----";
        assert_eq!(text(pem), pem);
        let mut value = json!("Bearer synthetic-secret");
        for _ in 0..32 {
            value = json!({"nested":value});
        }
        let mut safe = input_map(&value);
        for _ in 0..32 {
            safe = safe["nested"].clone();
        }
        assert_eq!(safe, DEPTH_REPLACEMENT);
    }

    #[test]
    fn secrets_and_nuls_are_scrubbed_even_inside_tool_inputs() {
        let token = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
        assert_eq!(
            text(&format!("read {token}\0")),
            "read [REDACTED GITHUB TOKEN]"
        );
        let input =
            json!({"token": {"items": [format!("{token}\0"), {"a\0b": "ok"}]}, "ab": "fixed"});
        let sanitized = input_map(&input);
        assert_eq!(sanitized["token"]["items"][0], "[REDACTED GITHUB TOKEN]");
        assert_eq!(sanitized["token"]["items"][1]["ab"], "ok");
        assert_eq!(
            input_map(&json!({"a\0b": 1, "ab": 2})),
            json!({"ab":2,"ab#2":1})
        );
    }
}
