//! Normalize JSON strings as Go's encoding/json decoder does before deserializing.

pub(super) fn normalize_strings(body: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(body.len());
    let mut offset = 0;
    let mut inside_string = false;
    while offset < body.len() {
        let byte = body[offset];
        if byte == b'"' {
            inside_string = !inside_string;
        }
        if inside_string
            && byte == b'\\'
            && body.get(offset + 1) == Some(&b'u')
            && let Some(code) = hex4(body.get(offset + 2..offset + 6))
        {
            let is_high = (0xd800..=0xdbff).contains(&code);
            let is_low = (0xdc00..=0xdfff).contains(&code);
            if is_high
                && body.get(offset + 6..offset + 8) == Some(b"\\u")
                && hex4(body.get(offset + 8..offset + 12))
                    .is_some_and(|next| (0xdc00..=0xdfff).contains(&next))
            {
                result.extend_from_slice(&body[offset..offset + 12]);
                offset += 12;
                continue;
            }
            if is_high || is_low {
                result.extend_from_slice("�".as_bytes());
                offset += 6;
                continue;
            }
        }
        if inside_string && byte == b'\\' {
            let end = (offset + 2).min(body.len());
            result.extend_from_slice(&body[offset..end]);
            offset = end;
            continue;
        }
        if inside_string && byte >= 0x80 {
            let width = match byte {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => 1,
            };
            if body
                .get(offset..offset + width)
                .is_some_and(|candidate| std::str::from_utf8(candidate).is_ok())
            {
                result.extend_from_slice(&body[offset..offset + width]);
                offset += width;
            } else {
                result.extend_from_slice("�".as_bytes());
                offset += 1;
            }
            continue;
        }
        result.push(byte);
        offset += 1;
    }
    result
}

pub(super) fn depth_ok(body: &[u8]) -> bool {
    if !matches!(
        body.iter().find(|byte| !byte.is_ascii_whitespace()),
        Some(b'{' | b'[')
    ) {
        return true;
    }
    let mut depth = 0usize;
    let mut inside_string = false;
    let mut escaped = false;
    for &byte in body {
        if inside_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                inside_string = false;
            }
        } else {
            match byte {
                b'"' => inside_string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 10_000 {
                        return false;
                    }
                }
                b'}' | b']' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        // json.Decoder.Decode does not validate later values.
                        return true;
                    }
                }
                _ => {}
            }
        }
    }
    true
}

fn hex4(bytes: Option<&[u8]>) -> Option<u16> {
    let bytes = bytes?;
    let mut result = 0u16;
    for &byte in bytes {
        let digit = (byte as char).to_digit(16)?;
        result = result * 16 + digit as u16;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_pairs_and_replaces_each_invalid_unit() {
        assert_eq!(
            normalize_strings(r#"{"name":"𐀀\ud800x\udc00"}"#.as_bytes()),
            r#"{"name":"𐀀�x�"}"#.as_bytes()
        );
        assert_eq!(
            normalize_strings(&[b'"', 0xe2, 0x82, b'"']),
            "\"��\"".as_bytes()
        );
        assert_eq!(
            normalize_strings(&[b'"', b'\\', b'"', 0xff, b'"']),
            "\"\\\"�\"".as_bytes()
        );
        assert_eq!(normalize_strings(br#""\\ud800""#), br#""\\ud800""#);
    }
}
