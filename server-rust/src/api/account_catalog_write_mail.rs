// Adapted from Go 1.26.9 net/mail/message.go, Copyright 2011 The Go Authors.
// See account_catalog_write_mail.LICENSE for BSD redistribution terms.
//! Address syntax accepted by Go's net/mail.ParseAddress for the cloud waitlist.
//! This is a syntax check, not DNS or deliverability validation.
use base64::{
    Engine, alphabet,
    engine::{GeneralPurpose, GeneralPurposeConfig},
};
use std::net::IpAddr;

pub(super) fn valid_address(input: &str) -> bool {
    let mut parser = Parser { remaining: input };
    parser.address(true) == Some(1) && parser.skip_cfws() && parser.remaining.is_empty()
}

struct Parser<'a> {
    remaining: &'a str,
}

impl<'a> Parser<'a> {
    fn space(&mut self) {
        self.remaining = self.remaining.trim_start_matches([' ', '\t']);
    }

    fn consume(&mut self, prefix: char) -> bool {
        if let Some(remaining) = self.remaining.strip_prefix(prefix) {
            self.remaining = remaining;
            true
        } else {
            false
        }
    }

    fn atom(&mut self, dots: bool, permissive: bool) -> Option<&'a str> {
        let size = self
            .remaining
            .char_indices()
            .take_while(|(_, ch)| {
                ch.is_ascii_graphic()
                    && !matches!(
                        ch,
                        '(' | ')' | '<' | '>' | '[' | ']' | ':' | ';' | '@' | '\\' | ',' | '"'
                    )
                    && (dots || *ch != '.')
                    || !ch.is_ascii() && (*ch as u32) >= 128
            })
            .last()
            .map(|(index, ch)| index + ch.len_utf8())?;
        let (value, rest) = self.remaining.split_at(size);
        self.remaining = rest;
        if !permissive && (value.starts_with('.') || value.ends_with('.') || value.contains("..")) {
            return None;
        }
        Some(value)
    }

    fn quoted(&mut self) -> bool {
        let Some(content) = self.remaining.strip_prefix('"') else {
            return false;
        };
        let mut escaped = false;
        for (index, ch) in content.char_indices() {
            if escaped {
                if !vchar(ch) && !matches!(ch, ' ' | '\t') {
                    return false;
                }
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                self.remaining = &content[index + 1..];
                return true;
            } else if !vchar(ch) && !matches!(ch, ' ' | '\t') {
                return false;
            }
        }
        false
    }

    fn addr_spec(&mut self) -> bool {
        let start = self.remaining;
        self.space();
        let local = if self.remaining.starts_with('"') {
            self.quoted()
        } else {
            self.atom(true, false).is_some()
        };
        // Go rejects an empty quoted local part but accepts an otherwise quoted one.
        // For the quoted branch, the check below uses the raw consumed span.
        let local = local && !(start.trim_start_matches([' ', '\t']).starts_with("\"\""));
        if !local || !self.consume('@') {
            self.remaining = start;
            return false;
        }
        self.space();
        let domain = if self.consume('[') {
            let Some(index) = self.remaining.find(']') else {
                self.remaining = start;
                return false;
            };
            let text = &self.remaining[..index];
            if !text
                .chars()
                .all(|ch| vchar(ch) && !matches!(ch, '[' | ']' | '\\'))
                || text.parse::<IpAddr>().is_err()
            {
                false
            } else {
                self.remaining = &self.remaining[index + 1..];
                true
            }
        } else {
            self.atom(true, false).is_some()
        };
        if !domain {
            self.remaining = start;
        }
        domain
    }

    fn comment(&mut self) -> Option<String> {
        let mut depth = 1usize;
        let mut escaped = false;
        let mut content = String::new();
        for (index, ch) in self.remaining.char_indices() {
            if escaped {
                content.push(ch);
                escaped = false;
                continue;
            }
            match ch {
                '\\' => {
                    escaped = true;
                    continue;
                }
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        self.remaining = &self.remaining[index + 1..];
                        return Some(content);
                    }
                }
                _ => {}
            }
            content.push(ch);
        }
        None
    }

    fn skip_cfws(&mut self) -> bool {
        self.space();
        while self.consume('(') {
            if self.comment().is_none() {
                return false;
            }
            self.space();
        }
        true
    }

    fn phrase(&mut self) -> bool {
        let mut words = 0;
        let mut encoded_buffer = false;
        let mut failed = false;
        loop {
            if words > 0 && !self.skip_cfws() {
                return false;
            }
            self.space();
            if self.remaining.is_empty() {
                break;
            }
            let word = if self.remaining.starts_with('"') {
                self.quoted().then_some(None).ok_or(())
            } else {
                self.atom(true, true).ok_or(()).and_then(encoded_word)
            };
            match word {
                Ok(Some(nonempty)) => encoded_buffer |= nonempty,
                Ok(None) => {
                    words += 1 + usize::from(encoded_buffer);
                    encoded_buffer = false;
                }
                Err(()) => {
                    failed = true;
                    break;
                }
            }
        }
        // Go 1.26 buffers adjacent encoded words separately from ordinary words.
        !failed || words + usize::from(encoded_buffer) > 0
    }

    fn address(&mut self, group: bool) -> Option<usize> {
        self.space();
        if self.remaining.is_empty() {
            return None;
        }
        if self.addr_spec() {
            self.space();
            if self.consume('(') {
                let comment = self.comment()?;
                if !comment
                    .split([' ', '\t'])
                    .all(|word| encoded_word(word).is_ok())
                {
                    return None;
                }
            }
            return Some(1);
        }
        if !self.remaining.starts_with('<') && !self.phrase() {
            return None;
        }
        self.space();
        if group && self.consume(':') {
            return self.group_list();
        }
        (self.consume('<') && self.addr_spec() && self.consume('>')).then_some(1)
    }

    fn group_list(&mut self) -> Option<usize> {
        self.space();
        if self.consume(';') {
            return self.skip_cfws().then_some(0);
        }
        let mut count = 0;
        loop {
            count += self.address(false)?;
            if !self.skip_cfws() {
                return None;
            }
            if self.consume(';') {
                return self.skip_cfws().then_some(count);
            }
            if !self.consume(',') {
                return None;
            }
        }
    }
}

fn vchar(ch: char) -> bool {
    ch.is_ascii_graphic() || ch as u32 >= 128
}

// net/mail treats malformed encoded words as plain text, but rejects a valid
// encoded word whose charset needs an unavailable CharsetReader.
fn encoded_word(word: &str) -> Result<Option<bool>, ()> {
    if word.len() < 8
        || !word.starts_with("=?")
        || !word.ends_with("?=")
        || word.bytes().filter(|byte| *byte == b'?').count() != 4
    {
        return Ok(None);
    }
    let mut parts = word[2..word.len() - 2].split('?');
    let charset = parts.next().unwrap_or("");
    let encoding = parts.next().unwrap_or("");
    let text = parts.next().unwrap_or("");
    if charset.is_empty() || encoding.len() != 1 {
        return Ok(None);
    }
    let decoded_len = match encoding.as_bytes()[0] {
        b'b' | b'B' => GeneralPurpose::new(
            &alphabet::STANDARD,
            GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
        )
        .decode(text.replace(['\r', '\n'], ""))
        .ok()
        .map(|data| data.len()),
        b'q' | b'Q' => valid_quoted_printable(text).then_some(text.len()),
        _ => None,
    };
    let Some(decoded_len) = decoded_len else {
        return Ok(None);
    };
    if !["utf-8", "iso-8859-1", "us-ascii"]
        .iter()
        .any(|known| charset.eq_ignore_ascii_case(known))
    {
        return Err(());
    }
    Ok(Some(decoded_len > 0))
}

fn valid_quoted_printable(text: &str) -> bool {
    let mut bytes = text.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'=' {
            if !bytes.next().is_some_and(|ch| ch.is_ascii_hexdigit())
                || !bytes.next().is_some_and(|ch| ch.is_ascii_hexdigit())
            {
                return false;
            }
        } else if !(b' '..=b'~').contains(&byte) && !matches!(byte, b'\r' | b'\n' | b'\t') {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_go_net_mail_oracle() {
        let cases: Vec<(String, bool)> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_mail_oracle.json"
        ))
        .unwrap();
        for (email, expected) in cases {
            assert_eq!(
                valid_address(&email),
                expected,
                "mail.ParseAddress({email:?})"
            );
        }
    }
}
