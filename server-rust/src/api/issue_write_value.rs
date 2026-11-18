//! Frozen internal/issueproperty/value.go value contract; no catalog writes.
use chrono::NaiveDate;
use serde_json::{Value, json};
use std::collections::HashSet;
use uuid::Uuid;

fn text(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Err("value cannot be empty (use DELETE to unset a property)".into());
    }
    if s.chars().count() > 2000 {
        return Err("value must be 2000 characters or fewer".into());
    }
    Ok(s.replace('\0', ""))
}

fn go_host_byte_allowed(byte: u8) -> bool {
    byte >= 0x80
        || byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
        )
}

fn valid_host_bytes(part: &str, zone: bool) -> bool {
    let bytes = part.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if bytes.get(index + 1).is_none_or(|b| !b.is_ascii_hexdigit())
                || bytes.get(index + 2).is_none_or(|b| !b.is_ascii_hexdigit())
            {
                return false;
            }
            let decoded = (bytes[index + 1] as char).to_digit(16).unwrap() * 16
                + (bytes[index + 2] as char).to_digit(16).unwrap();
            let decoded = decoded as u8;
            if zone {
                if decoded != b' ' && !go_host_byte_allowed(decoded) {
                    return false;
                }
            } else if decoded < 0x80 && &bytes[index..index + 3] != b"%25" {
                return false;
            }
            index += 3;
        } else {
            if !go_host_byte_allowed(bytes[index]) {
                return false;
            }
            index += 1;
        }
    }
    true
}

fn valid_url_escapes(part: &str) -> bool {
    let bytes = part.as_bytes();
    bytes.iter().enumerate().all(|(index, byte)| {
        *byte != b'%'
            || bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit)
    })
}

fn url(s: &str) -> Result<String, String> {
    let s = s.trim();
    if s.len() > 2048 {
        return Err("value must be 2048 characters or fewer".into());
    }
    // Go's net/url.Parse accepts numeric ports above 65535; WHATWG Url does
    // not. Validate the remaining URL with the port removed, but store the
    // original spelling exactly as Go does.
    let valid = s.split_once("://").is_some_and(|(scheme, rest)| {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        // net/url.Parse unescapes path and fragment, but leaves RawQuery
        // untouched. WHATWG Url accepts malformed escapes in all three.
        let remainder = &rest[authority.len()..];
        let (before_fragment, fragment) = remainder
            .split_once('#')
            .map_or((remainder, None), |(before, after)| (before, Some(after)));
        let path = before_fragment
            .split_once('?')
            .map_or(before_fragment, |(path, _)| path);
        if !valid_url_escapes(path) || fragment.is_some_and(|f| !valid_url_escapes(f)) {
            return false;
        }
        let (userinfo, host_port) = authority
            .rsplit_once('@')
            .map_or((None, authority), |(user, host)| (Some(user), host));
        // net/url.Parse validates percent escapes in Userinfo before returning
        // Host. WHATWG accepts malformed escapes here, so reject them explicitly.
        if userinfo.is_some_and(|user| {
            !valid_url_escapes(user)
                || !user.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || b"-._:~!$&'()*+,;=%@".contains(&byte)
                })
        }) {
            return false;
        }
        // Go 1.23 accepts scoped IPv6 and even bracketed non-address hosts.
        // Validate Go's host/zone escape and port rules, then substitute a
        // hostname solely for WHATWG validation. Store the original spelling.
        let candidate_input = if let Some(bracketed) = host_port.strip_prefix('[') {
            let Some(close) = bracketed.rfind(']') else {
                return false;
            };
            let literal = &bracketed[..close];
            let suffix = &bracketed[close + 1..];
            if !suffix.is_empty()
                && (!suffix.starts_with(':') || !suffix[1..].bytes().all(|b| b.is_ascii_digit()))
            {
                return false;
            }
            let (host_valid, zone_valid) = if let Some((host, zone)) = literal.split_once("%25") {
                (valid_host_bytes(host, false), valid_host_bytes(zone, true))
            } else {
                (valid_host_bytes(literal, false), true)
            };
            if !host_valid || !zone_valid {
                return false;
            }
            let userinfo = &authority[..authority.len() - host_port.len()];
            format!(
                "{scheme}://{userinfo}go-host{suffix}{}",
                &rest[authority.len()..]
            )
        } else if host_port.contains("%25") {
            // Go accepts a literal percent encoded in an unbracketed host,
            // while WHATWG rejects it. Substitute only for parsing; retain
            // the original URL for storage after Go's host escape guard below.
            let userinfo = &authority[..authority.len() - host_port.len()];
            format!(
                "{scheme}://{userinfo}{}{}",
                host_port.replace("%25", "go-pct"),
                &rest[authority.len()..]
            )
        } else {
            s.to_owned()
        };
        let (_, candidate_rest) = candidate_input.split_once("://").unwrap();
        let candidate_authority = candidate_rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        let candidate_host_port = candidate_authority
            .rsplit_once('@')
            .map_or(candidate_authority, |(_, host)| host);
        let port = candidate_host_port
            .rsplit_once(':')
            .and_then(|(host, port)| {
                (!host.is_empty()
                    && !port.is_empty()
                    && port.bytes().all(|b| b.is_ascii_digit())
                    && (port.len() > 5 || port.parse::<u16>().is_err()))
                .then_some(port)
            });
        let candidate = if let Some(hostless_port) = candidate_host_port.strip_prefix(':') {
            // Go tests parsed.Host, not parsed.Hostname: ":80" and ":" are
            // non-empty authorities. WHATWG rejects them without a hostname.
            if !hostless_port.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
            let userinfo =
                &candidate_authority[..candidate_authority.len() - candidate_host_port.len()];
            let port = if hostless_port.len() > 5
                || (!hostless_port.is_empty() && hostless_port.parse::<u16>().is_err())
            {
                ""
            } else {
                candidate_host_port
            };
            format!(
                "{scheme}://{userinfo}go-host{port}{}",
                &candidate_rest[candidate_authority.len()..]
            )
        } else {
            port.map_or_else(
                || candidate_input.clone(),
                |port| {
                    let cut = scheme.len() + 3 + candidate_authority.len() - port.len() - 1;
                    format!(
                        "{}{}",
                        &candidate_input[..cut],
                        &candidate_input[cut + port.len() + 1..]
                    )
                },
            )
        };
        matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https")
            && !authority.is_empty()
            && !authority.contains('\\')
            // net/url accepts a literal percent encoded as %25 in a host,
            // but rejects escapes that decode to ordinary host characters.
            && (host_port.starts_with('[') || !host_port.replace("%25", "").contains('%'))
            && !s.chars().any(char::is_control)
            && reqwest::Url::parse(&candidate).is_ok_and(|u| u.host_str().is_some())
    });
    if !valid {
        return Err("value must be an http(s) URL".into());
    }
    Ok(s.to_owned())
}

pub(super) fn actor_ref(s: &str) -> Result<String, String> {
    let Some((kind, id)) = s.split_once(':') else {
        return Err("value must look like \"<kind>:<uuid>\" where kind is one of: member".into());
    };
    if kind != "member" {
        return Err(format!("unknown actor kind {kind:?}; valid kinds: member"));
    }
    let id = Uuid::parse_str(id).map_err(|_| format!("actor id in {s:?} must be a UUID"))?;
    Ok(format!("member:{id}"))
}

fn string_list(value: &Value, item: fn(&str) -> Result<String, String>) -> Result<Value, String> {
    let items = value
        .as_array()
        .filter(|v| !v.is_empty())
        .ok_or("value must be a non-empty array of strings")?;
    if items.len() > 20 {
        return Err("value cannot list more than 20 entries".into());
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for v in items {
        let v = item(
            v.as_str()
                .ok_or("value must be a non-empty array of strings")?,
        )?;
        if seen.insert(v.clone()) {
            out.push(v);
        }
    }
    Ok(json!(out))
}

pub(super) fn validate(kind: &str, config: &Value, value: Option<&Value>) -> Result<Value, String> {
    let value = value.ok_or("value is required")?;
    if value.is_null() {
        return Err("value cannot be null (use DELETE to unset a property)".into());
    }
    let options = config
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let option_id = |v: &Value| {
        v.get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let mut hints: Vec<String> = options
        .iter()
        .take(20)
        .map(|v| {
            format!(
                "{} ({})",
                option_id(v),
                v.get("name").and_then(Value::as_str).unwrap_or_default()
            )
        })
        .collect();
    if options.len() > 20 {
        hints.push(format!(
            "... ({} more options; retrieve the property definition for the full list)",
            options.len() - 20
        ));
    }
    let hint = hints.join(", ");
    match kind {
        "text" => Ok(json!(text(
            value.as_str().ok_or("value must be a string")?
        )?)),
        "url" => Ok(json!(url(value
            .as_str()
            .ok_or("value must be a URL string")?)?)),
        "multi_text" => string_list(value, text),
        "multi_url" => string_list(value, url),
        "number" => {
            // Go unmarshals into float64, including rounding integers > 2^53.
            let number = value.as_f64().ok_or("value must be a number")?;
            if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
                Ok(json!(number as i64))
            } else {
                Ok(json!(number))
            }
        }
        "checkbox" => value
            .as_bool()
            .map(|v| json!(v))
            .ok_or_else(|| "value must be true or false".into()),
        "date" => {
            let msg = "value must be a date string in YYYY-MM-DD format";
            let s = value.as_str().ok_or(msg)?;
            if s.len() != 10
                || !s.bytes().enumerate().all(|(i, c)| {
                    if i == 4 || i == 7 {
                        c == b'-'
                    } else {
                        c.is_ascii_digit()
                    }
                })
                || NaiveDate::parse_from_str(s, "%Y-%m-%d").is_err()
            {
                return Err(msg.into());
            }
            Ok(value.clone())
        }
        "select" => {
            if value
                .as_str()
                .is_none_or(|id| !options.iter().any(|o| option_id(o) == id))
            {
                return Err(format!("value must be one of the option ids: {hint}"));
            }
            Ok(value.clone())
        }
        "multi_select" => {
            let msg = format!("value must be a non-empty array of option ids: {hint}");
            let items = value
                .as_array()
                .filter(|v| !v.is_empty())
                .ok_or(msg.clone())?;
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for item in items {
                let id = item.as_str().ok_or(msg.clone())?;
                let index = options
                    .iter()
                    .rposition(|v| option_id(v) == id)
                    .ok_or_else(|| format!("unknown option id {id:?}; valid option ids: {hint}"))?;
                if seen.insert(id) {
                    out.push((index, id));
                    if out.len() > 50 {
                        return Err("value cannot list more than 50 options".into());
                    }
                }
            }
            out.sort_by_key(|(index, _)| *index);
            Ok(json!(out.into_iter().map(|(_, id)| id).collect::<Vec<_>>()))
        }
        "actor" => Ok(json!(actor_ref(value.as_str().ok_or(
            "value must be an actor reference string like \"member:<uuid>\" (kinds: member)"
        )?)?)),
        "multi_actor" => {
            let items = value.as_array().ok_or("value must be an array of actor reference strings like \"member:<uuid>\" (kinds: member)")?;
            if items.is_empty() {
                return Err("value must be a non-empty array of actor references".into());
            }
            if items.len() > 20 {
                return Err("value cannot list more than 20 actors".into());
            }
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for item in items {
                let v = actor_ref(
                    item.as_str()
                        .ok_or("value must be an array of actor reference strings")?,
                )?;
                if seen.insert(v.clone()) {
                    out.push(v);
                }
            }
            Ok(json!(out))
        }
        _ => Err(format!("unsupported property type {kind:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn userinfo_matches_frozen_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../baseline/issue_write_userinfo_go1234.json"
        ))
        .unwrap();
        for case in cases {
            let input = case["input"].as_str().unwrap();
            assert_eq!(
                url(input).is_ok(),
                case["accepted"].as_bool().unwrap(),
                "{input}"
            );
        }
    }

    #[test]
    fn canonical_shapes_and_limits() {
        let cfg = json!({"options":[{"id":"b","name":"B"},{"id":"a","name":"A"}]});
        for (kind, input, expected) in [
            ("text", json!("  hi\0  "), json!("  hi  ")),
            (
                "multi_text",
                json!(["a\0", "a", " b "]),
                json!(["a", " b "]),
            ),
            (
                "multi_url",
                json!([" https://example.com ", "https://example.com"]),
                json!(["https://example.com"]),
            ),
            (
                "number",
                json!(9007199254740993_u64),
                json!(9007199254740992_i64),
            ),
            ("checkbox", json!(false), json!(false)),
            ("date", json!("2024-02-29"), json!("2024-02-29")),
            ("multi_select", json!(["a", "b", "a"]), json!(["b", "a"])),
            ("select", json!("b"), json!("b")),
            (
                "url",
                json!("HTTP://example.com:99999/path?key=value"),
                json!("HTTP://example.com:99999/path?key=value"),
            ),
            ("url", json!("http://:80/path"), json!("http://:80/path")),
            (
                "url",
                json!("http://example.com/?q=%ZZ"),
                json!("http://example.com/?q=%ZZ"),
            ),
            (
                "url",
                json!("http://example.com/%2F#%25"),
                json!("http://example.com/%2F#%25"),
            ),
            (
                "url",
                json!("http://[fe80::1%25en0]/"),
                json!("http://[fe80::1%25en0]/"),
            ),
            ("url", json!("http://[::1%25]/"), json!("http://[::1%25]/")),
            (
                "url",
                json!("http://[invalid%25eth0]/"),
                json!("http://[invalid%25eth0]/"),
            ),
            ("url", json!("http://[]/"), json!("http://[]/")),
            (
                "multi_url",
                json!([
                    "http://[fe80::1%25eth0]:99999/x",
                    "http://[fe80::1%25eth0%20wifi]/"
                ]),
                json!([
                    "http://[fe80::1%25eth0]:99999/x",
                    "http://[fe80::1%25eth0%20wifi]/"
                ]),
            ),
            ("url", json!("https://:"), json!("https://:")),
            (
                "url",
                json!("http://user%41@example.com/notes"),
                json!("http://user%41@example.com/notes"),
            ),
            (
                "multi_url",
                json!(["http://user@:99999/notes", "http://:80/path"]),
                json!(["http://user@:99999/notes", "http://:80/path"]),
            ),
            (
                "multi_url",
                json!([
                    "http://example.com:999999999999999999999999",
                    "https://example.com:99999"
                ]),
                json!([
                    "http://example.com:999999999999999999999999",
                    "https://example.com:99999"
                ]),
            ),
        ] {
            assert_eq!(validate(kind, &cfg, Some(&input)), Ok(expected), "{kind}");
        }
        for (kind, bad) in [
            ("text", json!("   ")),
            ("text", json!("a".repeat(2001))),
            ("number", json!("1")),
            ("checkbox", json!(1)),
            ("date", json!("2025-02-29")),
            ("date", json!("2025-2-01")),
            ("url", json!("https:example.com")),
            ("url", json!("http://example.com/path%ZZ")),
            ("url", json!("http://example.com/path%")),
            ("url", json!("http://example.com/#%ZZ")),
            ("url", json!("http://user%ZZ@example.com/path")),
            ("url", json!("http://user name@example.com")),
            ("url", json!("http://ü@example.com")),
            (
                "multi_url",
                json!(["https://example.com/ok", "https://a%ZZ@example.com/"]),
            ),
            (
                "multi_url",
                json!(["http://example.com/ok", "https://example.com/#%"]),
            ),
            ("url", json!("javascript:alert(1)")),
            ("url", json!("http://example.com:-1")),
            ("url", json!("http://:letters")),
            ("url", json!("http://[fe80::1%25eth0%ZZ]/")),
            ("url", json!("http://[fe80::1%eth0]/")),
            ("url", json!("http://[fe80::1%25eth0]:foo/")),
            ("url", json!("http://[abc/")),
            ("url", json!("http://[fe80::1%25a%00b]/")),
            ("url", json!("http://user@:80:bad")),
            ("url", json!("http://%41.example")),
            ("url", json!(r"http://foo\bar")),
            ("url", json!("http://example.com:99999:bad")),
            ("multi_text", json!([])),
            ("multi_text", json!(vec!["a"; 21])),
            ("multi_select", json!(["bad"])),
            ("actor", json!("agent:00000000-0000-0000-0000-000000000001")),
        ] {
            assert!(validate(kind, &cfg, Some(&bad)).is_err(), "{kind}: {bad}");
        }
        assert_eq!(
            validate("text", &cfg, None).unwrap_err(),
            "value is required"
        );
        assert_eq!(
            validate("text", &cfg, Some(&Value::Null)).unwrap_err(),
            "value cannot be null (use DELETE to unset a property)"
        );
        let upper = "member:12345678-ABCD-1234-ABCD-123456789ABC";
        assert_eq!(
            validate(
                "multi_actor",
                &cfg,
                Some(&json!([upper, upper.to_lowercase()]))
            )
            .unwrap(),
            json!([upper.to_lowercase()])
        );
    }
}

#[cfg(test)]
mod url_host_oracle {
    #[test]
    fn oracle() {
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../baseline/issue_write_url_go1234.json"))
                .unwrap();
        let mut failures = Vec::new();
        for case in cases {
            let input = case["input"].as_str().unwrap();
            let want = case["accepted"].as_bool().unwrap();
            let actual = super::url(input).is_ok();
            if actual != want {
                failures.push(format!("{input:?}: go={want} rust={actual}"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
