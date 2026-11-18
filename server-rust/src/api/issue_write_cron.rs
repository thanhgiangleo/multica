//! Five-field cron evaluation against an IANA timezone, using DB time as the anchor.
//! A local day can contain two occurrences of a minute or none at all.
use chrono::{DateTime, Datelike, Duration, LocalResult, Offset, TimeZone, Utc};
use chrono_tz::Tz;

const INVALID: &str = "cron must have a future occurrence";
const UNSUPPORTED: &str = "wakeup scheduler integration required";

#[derive(Clone)]
struct Field {
    allowed: Vec<bool>,
    wildcard: bool,
}

impl Field {
    fn parse(raw: &str, min: usize, max: usize, names: &[&str]) -> Option<Self> {
        let mut allowed = vec![false; max + 1];
        let mut wildcard = false;
        // robfig/cron ignores empty comma-separated ranges.
        for part in raw.split(',').filter(|part| !part.is_empty()) {
            let mut step_parts = part.split('/');
            let range = step_parts.next()?;
            let step = step_parts.next().map_or(Some(1), |v| {
                v.parse::<i64>().ok().and_then(|v| usize::try_from(v).ok())
            })?;
            if step_parts.next().is_some() || step == 0 {
                return None;
            }
            let value = |s: &str| -> Option<usize> {
                names
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(s))
                    .map(|index| index + min)
                    .or_else(|| s.parse::<i64>().ok().and_then(|v| usize::try_from(v).ok()))
                    .filter(|v| (min..=max).contains(v))
            };
            let mut range_parts = range.split('-');
            let first = range_parts.next()?;
            let (start, mut end, star) = if matches!(first, "*" | "?") {
                (min, max, true)
            } else {
                let start = value(first)?;
                let end = range_parts.next().map_or(Some(start), value)?;
                (start, end, false)
            };
            if !matches!(first, "*" | "?") && range_parts.next().is_some() {
                return None;
            }
            if part.contains('/') && !range.contains('-') {
                end = max;
            }
            if start > end {
                return None;
            }
            wildcard |= star && step == 1;
            for i in (start..=end).step_by(step) {
                allowed[i] = true;
            }
        }
        Some(Self { allowed, wildcard })
    }

    fn has(&self, value: usize) -> bool {
        self.allowed[value]
    }
}

/// Return next UTC instant strictly after `after`, including both fall-back folds.
/// On spring-forward gaps the nonexistent wall time has no occurrence.
/// Go's robfig/cron bounds search by the local start year plus five years.
pub(crate) fn next_occurrence(
    expression: &str,
    timezone: &str,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>, &'static str> {
    let mut parts = expression.split_whitespace();
    let embedded_zone = if expression.starts_with("TZ=") || expression.starts_with("CRON_TZ=") {
        parts.next().and_then(|first| {
            first
                .strip_prefix("CRON_TZ=")
                .or_else(|| first.strip_prefix("TZ="))
        })
    } else {
        None
    };
    let zone: Tz = if let Some(zone) = embedded_zone {
        // Go's parser accepts an embedded timezone, overriding the column for
        // schedule evaluation while retaining the column for display.
        zone.parse().map_err(|_| INVALID)?
    } else {
        timezone.parse().map_err(|_| INVALID)?
    };
    let fields: Vec<_> = parts.collect();
    if fields.len() != 5 {
        return Err(INVALID);
    }
    let months = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let weekdays = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
    let [
        Some(minute),
        Some(hour),
        Some(day),
        Some(month),
        Some(weekday),
    ] = [
        Field::parse(fields[0], 0, 59, &[]),
        Field::parse(fields[1], 0, 23, &[]),
        Field::parse(fields[2], 1, 31, &[]),
        Field::parse(fields[3], 1, 12, &months),
        Field::parse(fields[4], 0, 6, &weekdays),
    ]
    else {
        return Err(INVALID);
    };
    let start = after.with_timezone(&zone);
    let year_limit = start.year() + 5;
    // robfig/cron increments whole hours and realigns midnight when crossing
    // DST. On sub-hour transitions it can skip a day or jump to a different
    // local hour; evaluating civil minute candidates would silently misfire.
    // Fail closed for such zones until the Go increment semantics are ported.
    let mut previous = None;
    for year in start.year()..=year_limit {
        for month in 1..=12 {
            let probe = Utc
                .with_ymd_and_hms(year, month, 15, 12, 0, 0)
                .single()
                .ok_or(INVALID)?;
            let seconds = probe.with_timezone(&zone).offset().fix().local_minus_utc();
            if previous.is_some_and(|offset: i32| (seconds - offset) % 3600 != 0) {
                return Err(UNSUPPORTED);
            }
            previous = Some(seconds);
        }
    }
    let mut date = start.date_naive();
    while date.year() <= year_limit {
        let dom = day.has(date.day() as usize);
        let dow = weekday.has(date.weekday().num_days_from_sunday() as usize);
        let matches_day = if day.wildcard || weekday.wildcard {
            dom && dow
        } else {
            dom || dow
        };
        if month.has(date.month() as usize) && matches_day {
            let mut earliest = None;
            for h in 0..24 {
                if !hour.has(h) {
                    continue;
                }
                for m in 0..60 {
                    if !minute.has(m) {
                        continue;
                    }
                    let local = date.and_hms_opt(h as u32, m as u32, 0).ok_or(INVALID)?;
                    let choices = match zone.from_local_datetime(&local) {
                        LocalResult::Single(t) => [Some(t), None],
                        LocalResult::Ambiguous(a, b) => [Some(a), Some(b)],
                        LocalResult::None => [None, None],
                    };
                    for t in choices.into_iter().flatten() {
                        let utc = t.to_utc();
                        if utc > after && earliest.is_none_or(|best| utc < best) {
                            earliest = Some(utc);
                        }
                    }
                }
            }
            if let Some(next) = earliest {
                return Ok(next);
            }
        }
        date = date.checked_add_signed(Duration::days(1)).ok_or(INVALID)?;
    }
    Err(INVALID)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn cron_fields_and_strict_next() {
        let after = Utc.with_ymd_and_hms(2026, 11, 1, 8, 59, 0).unwrap();
        assert_eq!(
            next_occurrence("0 9 * * *", "UTC", after).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 1, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 * * *", "UTC", after + Duration::minutes(1)).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("*/15 9-10 * jan-mar mon-fri", "UTC", after).unwrap(),
            Utc.with_ymd_and_hms(2027, 1, 1, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 2 * mon", "UTC", after).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 ? * mon", "UTC", after).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 2,* * mon", "UTC", after).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 */2 * mon", "UTC", after + Duration::minutes(1)).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap()
        );
    }

    #[test]
    fn zones_and_dst() {
        let at = Utc.with_ymd_and_hms(2026, 3, 8, 6, 0, 0).unwrap();
        // New York has no 02:30 on spring-forward day.
        assert_eq!(
            next_occurrence("30 2 * * *", "America/New_York", at).unwrap(),
            Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap()
        );
        let at = Utc.with_ymd_and_hms(2026, 11, 1, 5, 0, 0).unwrap();
        let first = next_occurrence("30 1 * * *", "America/New_York", at).unwrap();
        assert_eq!(first, Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap());
        assert_eq!(
            next_occurrence("30 1 * * *", "America/New_York", first).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 1, 6, 30, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("0 9 * * *", "Asia/Ho_Chi_Minh", at).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 2, 0, 0).unwrap()
        );
        assert_eq!(
            next_occurrence("CRON_TZ=Asia/Tokyo 0 9 * * *", "UTC", at).unwrap(),
            Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn frozen_go_oracle() {
        let samples: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_cron_oracle.json")).unwrap();
        let mut failures = Vec::new();
        let mut compared = 0;
        let mut timed_out = 0;
        let mut unsupported = 0;
        for case in samples {
            if case["timed_out"] == true {
                timed_out += 1;
                continue;
            }
            let after = DateTime::parse_from_rfc3339(case["after"].as_str().unwrap())
                .unwrap()
                .to_utc();
            let next = next_occurrence(
                case["expression"].as_str().unwrap(),
                case["timezone"].as_str().unwrap(),
                after,
            );
            if case["timezone"] == "Australia/Lord_Howe" {
                unsupported += 1;
                if next.is_ok() {
                    failures.push(format!("{case}: sub-hour DST must fail closed"));
                }
                continue;
            }
            compared += 1;
            let expected = case["next"]
                .as_str()
                .map(|s| DateTime::parse_from_rfc3339(s).unwrap().to_utc());
            if next.as_ref().ok() != expected.as_ref() {
                failures.push(format!("{case}: Rust {next:?}"));
            }
        }
        assert_eq!(compared, 382);
        assert_eq!(unsupported, 62);
        assert_eq!(timed_out, 1, "Go oracle timeouts are not parity evidence");
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn invalid_cron_never_registers() {
        let now = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
        for bad in [
            "TZ=",
            "@daily",
            "0 9 * *",
            "60 9 * * *",
            "0 9 30 2 *",
            "*/0 9 * * *",
            "0 9 * 13 *",
            "0 9 * * 7",
            "0 9 * * mon-wed-fri",
            "TZ=Bad/Zone 0 9 * * *",
        ] {
            assert!(next_occurrence(bad, "UTC", now).is_err(), "{bad}");
        }
    }
}
