//! Calendar input shared by issue create, update, batch, and quick-create.
use chrono::{DateTime, Duration, NaiveDate, Timelike, Utc};

// Go util.ParseCalendarDate accepts a date-only string or an RFC3339 instant
// at exactly UTC midnight. A local midnight that maps to another UTC instant
// is ambiguous and must not silently shift the stored calendar day.
pub(in crate::api) fn parse(raw: &str) -> Option<NaiveDate> {
    let bytes = raw.as_bytes();
    if bytes.len() == 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..].iter().all(u8::is_ascii_digit)
        && let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
    {
        return Some(date);
    }
    // Go requires a capital T and Z; Chrono also accepts lowercase variants.
    if bytes.get(10) != Some(&b'T') || bytes.last() == Some(&b'z') {
        return None;
    }
    // Go's time.RFC3339 parser also accepts comma fractions and offsets up to
    // 24:60. Chrono rejects the latter, so parse the local clock separately
    // and apply the offset before checking the UTC calendar boundary.
    let normalized = raw.replace(',', ".");
    let instant = if let Ok(instant) = DateTime::parse_from_rfc3339(&normalized) {
        instant.with_timezone(&Utc)
    } else {
        let boundary = normalized.len().checked_sub(6)?;
        let local = normalized.get(..boundary)?;
        let offset = normalized.get(boundary..)?.as_bytes();
        if !matches!(offset[0], b'+' | b'-')
            || offset[3] != b':'
            || !offset[1..3].iter().all(u8::is_ascii_digit)
            || !offset[4..6].iter().all(u8::is_ascii_digit)
        {
            return None;
        }
        let hours = (offset[1] - b'0') * 10 + offset[2] - b'0';
        let minutes = (offset[4] - b'0') * 10 + offset[5] - b'0';
        if hours > 24 || minutes > 60 {
            return None;
        }
        let clock = DateTime::parse_from_rfc3339(&format!("{local}Z"))
            .ok()?
            .with_timezone(&Utc);
        let delta = Duration::minutes(i64::from(hours) * 60 + i64::from(minutes));
        clock.checked_add_signed(if offset[0] == b'+' { -delta } else { delta })?
    };
    if instant.time().hour() == 0
        && instant.time().minute() == 0
        && instant.time().second() == 0
        && instant.time().nanosecond() == 0
    {
        Some(instant.date_naive())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::parse;
    #[test]
    fn accepts_only_unambiguous_go_calendar_dates() {
        for (raw, date) in [
            ("2026-03-01", "2026-03-01"),
            ("2026-03-01T00:00:00Z", "2026-03-01"),
            ("2026-03-01T07:00:00+07:00", "2026-03-01"),
            ("2026-02-28T19:00:00-05:00", "2026-03-01"),
            ("2026-03-01T00:00:00,000Z", "2026-03-01"),
            ("2026-03-02T00:00:00+24:00", "2026-03-01"),
            ("2026-03-02T01:00:00+00:60", "2026-03-02"),
            ("2026-03-02T00:00:00-24:00", "2026-03-03"),
            (
                "2026-03-01T00:00:00.0000000000000000000000000000001Z",
                "2026-03-01",
            ),
            (
                "2026-03-02T00:00:00,0000000000000000000000000000001+24:00",
                "2026-03-01",
            ),
        ] {
            assert_eq!(parse(raw).unwrap().to_string(), date, "{raw}");
        }
        for raw in [
            "2026-03-01T00:00:01Z",
            "2026-03-01T00:00:00.000000001Z",
            "2026-03-01T00:00:00+07:00",
            "2026-03-01T00:00:00.0000000010000000000Z",
            "2026-03-01T00:00:00.0000000000000000000,0Z",
            "2026-03-01t00:00:00z",
            "2026-03-01T00:00:00z",
            "2026-03-01T00:00:00🦀:00",
            "2026-03-01t00:00:00Z",
            "2026-03-01T00:00:00+25:00",
            "2026-03-01T00:00:00+00:61",
            "2026-03-01T00:00:00",
            "2026-02-29",
            "2026-3-1",
            "+2026-03-01",
            "2026-03-01extra",
        ] {
            assert!(parse(raw).is_none(), "{raw}");
        }
    }
}
