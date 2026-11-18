//! Decode Go `json.RawMessage` struct fields without changing their source bytes.
use super::super::issue_write_comment_decode::{folded, normalized_strings};
use serde::de;

fn skip_space(body: &[u8], index: &mut usize) {
    while body.get(*index).is_some_and(u8::is_ascii_whitespace) {
        *index += 1;
    }
}

fn string_end(body: &[u8], start: usize) -> usize {
    let mut index = start + 1;
    while index < body.len() {
        match body[index] {
            b'\\' => index += 2,
            b'"' => return index + 1,
            _ => index += 1,
        }
    }
    unreachable!("JSON syntax was validated")
}

fn value_end(body: &[u8], start: usize) -> usize {
    let mut index = start;
    let mut depth = 0;
    while index < body.len() {
        match body[index] {
            b'"' => index = string_end(body, index),
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' | b']' if depth == 0 => break,
            b'}' | b']' => {
                depth -= 1;
                index += 1;
            }
            b',' if depth == 0 => break,
            byte if depth == 0 && byte.is_ascii_whitespace() => break,
            _ => index += 1,
        }
    }
    index
}

/// Check syntax before exposing any fields. Go's Decoder reads the first JSON
/// value even if more bytes follow; typed-field errors can then be ignored by
/// a caller without discarding the other fields or their original order.
pub(in crate::api) fn fields(
    body: &[u8],
    mut visit: impl FnMut(&str, &[u8]),
) -> Result<(), serde_json::Error> {
    let normalized = normalized_strings(body);
    let mut stream =
        serde_json::Deserializer::from_slice(&normalized).into_iter::<de::IgnoredAny>();
    stream
        .next()
        .ok_or_else(|| de::Error::custom("missing JSON value"))??;
    let mut index = 0;
    skip_space(body, &mut index);
    if body[index] == b'n' {
        return Ok(());
    }
    if body[index] != b'{' {
        return Err(de::Error::custom("expected an object or null"));
    }
    index += 1;
    loop {
        skip_space(body, &mut index);
        if body[index] == b'}' {
            return Ok(());
        }
        let end = string_end(body, index);
        let key: String = serde_json::from_slice(&normalized_strings(&body[index..end]))?;
        index = end;
        skip_space(body, &mut index);
        index += 1; // colon; checked by JSON syntax validation above
        skip_space(body, &mut index);
        let end = value_end(body, index);
        visit(&folded(&key), &body[index..end]);
        index = end;
        skip_space(body, &mut index);
        if body[index] == b'}' {
            return Ok(());
        }
        index += 1; // comma
    }
}

/// Go Decoder consumes one JSON value, and RawMessage holds the *original*
/// bytes. Normalizing the full body before extracting RawValue would silently
/// alter invalid UTF-8 and unpaired surrogates and change the DB write result.
pub(in crate::api) fn raw_field(
    body: &[u8],
    field: &str,
) -> Result<Option<Vec<u8>>, serde_json::Error> {
    let mut result = None;
    fields(body, |key, value| {
        if key == field {
            result = Some(value.to_vec());
        }
    })?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_field_preserves_source_bytes_and_struct_key_order() {
        for (body, expected) in [
            (
                br#"{"value":1,"VALUE":2}"#.as_slice(),
                Some(b"2".as_slice()),
            ),
            (br#"{"value":{"a":["}",1]},"value":true}"#, Some(b"true")),
            (br#"{"ignored":{"value":1},"value":null}"#, Some(b"null")),
            (br#"{"value":"\ud800"}"#, Some(br#""\ud800""#)),
            (b"{\"value\":\"\xff\"}", Some(b"\"\xff\"")),
            (br#"{"value":9} garbage"#, Some(b"9")),
            (br#"null"#, None),
        ] {
            assert_eq!(raw_field(body, "value").unwrap().as_deref(), expected);
        }
        assert!(raw_field(b"{\"value\":", "value").is_err());
        assert!(raw_field(b"[]", "value").is_err());
    }
}
