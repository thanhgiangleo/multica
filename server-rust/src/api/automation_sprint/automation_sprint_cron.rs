//! Five-field cron with robfig/cron v3.0.1 field and increment semantics.
//! Civil candidate enumeration is insufficient for sub-hour DST transitions.
use super::{ApiError, GoJson, bad, error};
use axum::{extract::RawQuery, http::StatusCode};
use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Offset, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde_json::{Value, json};

const INVALID: &str = "invalid cron expression";
const EXHAUSTED: &str = "cron evaluation limit exceeded";

pub(super) fn timezone(value: &str) -> Result<(), ApiError> {
    value
        .parse::<Tz>()
        .map(|_| ())
        .map_err(|_| bad("invalid timezone"))
}

struct Field {
    bits: u64,
    star: bool,
}

impl Field {
    fn parse(raw: &str, min: usize, max: usize, names: &[&str]) -> Result<Self, &'static str> {
        let number = |raw: &str| -> Result<usize, &'static str> {
            names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(raw))
                .map(|i| i + min)
                .or_else(|| {
                    raw.parse::<i64>()
                        .ok()
                        .and_then(|v| usize::try_from(v).ok())
                })
                .filter(|value| (min..=max).contains(value))
                .ok_or(INVALID)
        };
        let mut field = Self {
            bits: 0,
            star: false,
        };
        for part in raw.split(',').filter(|part| !part.is_empty()) {
            let steps: Vec<_> = part.split('/').collect();
            if steps.len() > 2 {
                return Err(INVALID);
            }
            let bounds: Vec<_> = steps[0].split('-').collect();
            let wildcard = matches!(bounds[0], "*" | "?");
            let (start, mut end) = if wildcard {
                // robfig intentionally ignores trailing range text after a star.
                (min, max)
            } else {
                let start = number(bounds[0])?;
                match bounds.len() {
                    1 => (start, start),
                    2 => (start, number(bounds[1])?),
                    _ => return Err(INVALID),
                }
            };
            let step = if steps.len() == 2 {
                if bounds.len() == 1 {
                    end = max;
                }
                steps[1]
                    .parse::<i64>()
                    .ok()
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n > 0)
                    .ok_or(INVALID)?
            } else {
                1
            };
            if start > end {
                return Err(INVALID);
            }
            field.star |= wildcard && step == 1;
            for value in start..=end {
                if (value - start) % step == 0 {
                    field.bits |= 1 << value;
                }
            }
        }
        Ok(field)
    }
    fn has(&self, value: u32) -> bool {
        self.bits & (1 << value) != 0
    }
}

pub(super) struct Schedule {
    minute: Field,
    hour: Field,
    day: Field,
    month: Field,
    weekday: Field,
    zone: Tz,
}

impl Schedule {
    pub(super) fn parse(expression: &str, zone: &str) -> Result<Self, &'static str> {
        let mut zone = zone.parse::<Tz>().map_err(|_| INVALID)?;
        let expression = if expression.starts_with("TZ=") || expression.starts_with("CRON_TZ=") {
            let (prefix, rest) = expression.split_once(' ').ok_or(INVALID)?;
            let (_, name) = prefix.split_once('=').ok_or(INVALID)?;
            zone = name.parse::<Tz>().map_err(|_| INVALID)?;
            rest.trim()
        } else {
            expression
        };
        let fields: Vec<_> = expression.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(INVALID);
        }
        Ok(Self {
            minute: Field::parse(fields[0], 0, 59, &[])?,
            hour: Field::parse(fields[1], 0, 23, &[])?,
            day: Field::parse(fields[2], 1, 31, &[])?,
            month: Field::parse(
                fields[3],
                1,
                12,
                &[
                    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
                    "dec",
                ],
            )?,
            weekday: Field::parse(
                fields[4],
                0,
                6,
                &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
            )?,
            zone,
        })
    }

    fn day_matches(&self, t: DateTime<Tz>) -> bool {
        let dom = self.day.has(t.day());
        let dow = self.weekday.has(t.weekday().num_days_from_sunday());
        if self.day.star || self.weekday.star {
            dom && dow
        } else {
            dom || dow
        }
    }

    /// Match Go time.Date's offset choice for both ambiguous and missing times.
    fn local(&self, naive: NaiveDateTime) -> Result<DateTime<Tz>, &'static str> {
        let wall = naive.and_utc();
        let offset = self
            .zone
            .offset_from_utc_datetime(&naive)
            .fix()
            .local_minus_utc();
        let probe = wall
            .checked_sub_signed(Duration::seconds(offset.into()))
            .ok_or(EXHAUSTED)?;
        let corrected = self
            .zone
            .offset_from_utc_datetime(&probe.naive_utc())
            .fix()
            .local_minus_utc();
        let utc = wall
            .checked_sub_signed(Duration::seconds(corrected.into()))
            .ok_or(EXHAUSTED)?;
        Ok(utc.with_timezone(&self.zone))
    }

    /// Strict next instant; None is a valid expression with no future activation.
    pub(super) fn next(&self, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, &'static str> {
        if [&self.minute, &self.hour, &self.month]
            .iter()
            .any(|f| f.bits == 0)
            || (self.day.bits == 0 && self.weekday.bits == 0)
        {
            return Ok(None);
        }
        let mut t = after
            .checked_add_signed(Duration::seconds(1))
            .ok_or(EXHAUSTED)?
            .with_nanosecond(0)
            .ok_or(EXHAUSTED)?
            .with_timezone(&self.zone);
        let limit = t.year() + 5;
        let mut added = false;
        let mut budget = 100_000usize;
        let mut spend = || {
            budget = budget.checked_sub(1).ok_or(EXHAUSTED)?;
            Ok::<_, &'static str>(())
        };
        'wrap: loop {
            spend()?;
            if t.year() > limit {
                return Ok(None);
            }
            while !self.month.has(t.month()) {
                spend()?;
                if !added {
                    added = true;
                    let date = t.date_naive().with_day(1).ok_or(EXHAUSTED)?;
                    t = self.local(date.and_hms_opt(0, 0, 0).ok_or(EXHAUSTED)?)?;
                }
                let (year, month) = if t.month() == 12 {
                    (t.year() + 1, 1)
                } else {
                    (t.year(), t.month() + 1)
                };
                let date = chrono::NaiveDate::from_ymd_opt(year, month, 1).ok_or(EXHAUSTED)?;
                t = self.local(
                    date.and_hms_opt(t.hour(), t.minute(), t.second())
                        .ok_or(EXHAUSTED)?,
                )?;
                if t.month() == 1 {
                    continue 'wrap;
                }
            }
            while !self.day_matches(t) {
                spend()?;
                if !added {
                    added = true;
                    t = self.local(t.date_naive().and_hms_opt(0, 0, 0).ok_or(EXHAUSTED)?)?;
                }
                let date = t.date_naive().succ_opt().ok_or(EXHAUSTED)?;
                t = self.local(
                    date.and_hms_opt(t.hour(), t.minute(), t.second())
                        .ok_or(EXHAUSTED)?,
                )?;
                if t.hour() != 0 {
                    let delta = if t.hour() > 12 {
                        24 - t.hour() as i64
                    } else {
                        -(t.hour() as i64)
                    };
                    t = (t + Duration::hours(delta)).with_timezone(&self.zone);
                }
                if t.day() == 1 {
                    continue 'wrap;
                }
            }
            while !self.hour.has(t.hour()) {
                spend()?;
                if !added {
                    added = true;
                    t = self.local(
                        t.date_naive()
                            .and_hms_opt(t.hour(), 0, 0)
                            .ok_or(EXHAUSTED)?,
                    )?;
                }
                t = (t + Duration::hours(1)).with_timezone(&self.zone);
                if t.hour() == 0 {
                    continue 'wrap;
                }
            }
            while !self.minute.has(t.minute()) {
                spend()?;
                if !added {
                    added = true;
                    t = t
                        .to_utc()
                        .with_second(0)
                        .ok_or(EXHAUSTED)?
                        .with_timezone(&self.zone);
                }
                t = (t + Duration::minutes(1)).with_timezone(&self.zone);
                if t.minute() == 0 {
                    continue 'wrap;
                }
            }
            if t.second() != 0 {
                added = true;
                t = (t + Duration::seconds(60 - t.second() as i64)).with_timezone(&self.zone);
                continue 'wrap;
            }
            return Ok(Some(t.to_utc()));
        }
    }
}

pub(super) fn next(
    expression: &str,
    zone: &str,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>, ApiError> {
    let zone = if zone.is_empty() { "UTC" } else { zone };
    timezone(zone)?;
    let schedule = Schedule::parse(expression, zone).map_err(|_| bad(INVALID))?;
    match schedule.next(after) {
        Ok(Some(next)) => Ok(next),
        // Go ComputeNextRun persists its zero time for a never-firing schedule.
        Ok(None) => Ok(Utc
            .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
            .single()
            .expect("Go zero time")),
        Err(_) => Err(error(StatusCode::SERVICE_UNAVAILABLE, EXHAUSTED)),
    }
}

pub(super) async fn preview(RawQuery(query): RawQuery) -> (StatusCode, GoJson<Value>) {
    let values = super::parse_params(query.as_deref());
    let expression = values.get("expr").map(String::as_str).unwrap_or("");
    if expression.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            GoJson(json!({"error":"expr is required","code":"invalid_cron"})),
        );
    }
    let zone = values
        .get("tz")
        .filter(|v| !v.is_empty())
        .map(String::as_str)
        .unwrap_or("UTC");
    if timezone(zone).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            GoJson(json!({"error":"invalid timezone","code":"invalid_timezone"})),
        );
    }
    let schedule = match Schedule::parse(expression, zone) {
        Ok(schedule) => schedule,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                GoJson(json!({"error":INVALID,"code":"invalid_cron"})),
            );
        }
    };
    let mut cursor = Utc::now();
    let mut times = Vec::new();
    for _ in 0..3 {
        match schedule.next(cursor) {
            Ok(Some(value)) => {
                times.push(value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
                cursor = value;
            }
            Ok(None) => break,
            Err(_) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    GoJson(json!({"error":EXHAUSTED})),
                );
            }
        }
    }
    (StatusCode::OK, GoJson(json!({"next_runs":times})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frozen_go_oracle_including_subhour_dst() {
        let samples: Vec<Value> =
            serde_json::from_str(include_str!("../../../tests/issue_write_cron_oracle.json"))
                .unwrap();
        let mut failures = Vec::new();
        let mut compared = 0;
        for case in samples {
            if case["timed_out"] == true {
                continue;
            }
            let after = DateTime::parse_from_rfc3339(case["after"].as_str().unwrap())
                .unwrap()
                .to_utc();
            let next = Schedule::parse(
                case["expression"].as_str().unwrap(),
                case["timezone"].as_str().unwrap(),
            )
            .and_then(|schedule| schedule.next(after));
            let expected = case["next"]
                .as_str()
                .map(|s| DateTime::parse_from_rfc3339(s).unwrap().to_utc());
            if next.as_ref().ok().copied().flatten() != expected {
                failures.push(format!("{case}: Rust {next:?}"));
            }
            compared += 1;
        }
        assert_eq!(compared, 444);
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
    #[tokio::test]
    async fn preview_distinguishes_never_from_invalid() {
        let (status, GoJson(body)) = preview(RawQuery(Some("expr=0+0+30+2+*".into()))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"next_runs":[]}));
        let (status, GoJson(body)) = preview(RawQuery(Some("expr=60+0+*+*+*".into()))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_cron");
    }
    #[test]
    fn never_firing_trigger_uses_go_zero_time() {
        let after = Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap();
        assert_eq!(next("0 0 30 2 *", "UTC", after).unwrap().year(), 1);
        assert!(next("0 0 * * *", "Not/AZone", after).is_err());
    }
}
