// Generate synthetic oracle fixtures using the frozen service/cron.go unchanged
// except for its package declaration. See issue_write_cron_oracle.py.
package main

import (
    "encoding/json"
    "os"
    "time"
)

type sample struct {
    Expression string `json:"expression"`
    Timezone string `json:"timezone"`
    After string `json:"after"`
    Next string `json:"next,omitempty"`
    Invalid bool `json:"invalid,omitempty"`
    TimedOut bool `json:"timed_out,omitempty"`
}

func main() {
    cases := []sample{}
    add := func(expression, zone, after string) {
        t, err := time.Parse(time.RFC3339Nano, after)
        if err != nil { panic(err) }
        c := sample{Expression: expression, Timezone: zone, After: after}
        outcome := make(chan sample, 1)
        go func() {
            result := sample{Expression: expression, Timezone: zone, After: after}
            next, err := NextOccurrenceAfterUTC(expression, zone, t)
            if err != nil || next.IsZero() {
                result.Invalid = true
            } else {
                result.Next = next.Format(time.RFC3339Nano)
            }
            outcome <- result
        }()
        select {
        case c = <-outcome:
        case <-time.After(200 * time.Millisecond):
            c = sample{Expression: expression, Timezone: zone, After: after, TimedOut: true}
        }
        cases = append(cases, c)
    }
    expressions := []string{
        "* * * * *", "*/15 * * * *", "0 9 * * *", "30 1 * * *", "30 2 * * *",
        "0 0 * * *", "0 12 29 2 *", "0 9 2 * mon", "0 9 ? * mon",
        "0 9 2,* * mon", "0 9 */2 * mon", "0 9 *,2/3 * mon", "1-59/7 1,4-7 * * *",
        "0,30 0-23/3 * JAN-MAR MON-FRI", "0 9 30 2 *", "0 9 31 2 mon",
        "59 23 31 dec *", "1/20 1/3 * * *", "+1 +9 * * *", "0 9 * * 0,6",
        "0 9 * * SUN", "0 9 * * ?", "0 9 ? * ?", "0 9 ,2,, * mon",
    }
    for _, zone := range []string{"UTC", "Etc/UTC", "Asia/Ho_Chi_Minh", "Asia/Kathmandu", "America/New_York", "Europe/Berlin", "Australia/Lord_Howe"} {
        for _, expression := range expressions {
            for _, after := range []string{"2026-01-01T00:00:00Z", "2026-11-01T05:30:00.000000001Z"} {
                add(expression, zone, after)
            }
        }
    }
    for _, tc := range []struct{ zone, before string }{
        {"America/New_York", "2026-03-08T06:59:59.999999999Z"},
        {"America/New_York", "2026-11-01T05:29:59Z"},
        {"America/New_York", "2026-11-01T05:30:00Z"},
        {"America/New_York", "2026-11-01T06:00:00Z"},
        {"Europe/Berlin", "2026-03-29T00:59:59Z"},
        {"Europe/Berlin", "2026-10-25T00:30:00Z"},
        {"Australia/Lord_Howe", "2026-04-04T14:45:00Z"},
        {"Australia/Lord_Howe", "2026-10-03T15:20:00Z"},
        {"Pacific/Apia", "2011-12-29T09:00:00Z"},
        {"America/Sao_Paulo", "2018-11-03T02:00:00Z"},
        {"Africa/Casablanca", "2026-02-15T01:30:00Z"},
        {"Pacific/Chatham", "2026-04-04T13:45:00Z"},
    } {
        for _, expression := range []string{"* * * * *", "30 1 * * *", "45 1 * * *", "30 2 * * *", "0 0 * * *", "15 0 * * sun", "0 9 * * *"} {
            add(expression, tc.zone, tc.before)
        }
    }
    for _, expression := range []string{
        "", "TZ=", "CRON_TZ=Asia/Tokyo", "TZ=Bad/Zone 0 9 * * *", "@daily", "@every 1h",
        "0 9 * *", "0 9 * * * *", "60 9 * * *", "0 24 * * *", "0 9 0 * *", "0 9 * 13 *", "0 9 * * 7",
        "*/0 9 * * *", "*/-1 9 * * *", "0 9 * * mon-wed-fri", "0//2 9 * * *", "0 9 * * frizz",
        "TZ=Asia/Tokyo 0 9 * * *", "CRON_TZ=America/New_York 30 1 * * *",
        "  CRON_TZ=Asia/Tokyo 0 9 * * *", "0 9 * * 1,", "0 9 * * ,",
        "0 9 * * *-bad", "0 9 * * */18446744073709551615",
    } {
        add(expression, "UTC", "2026-11-01T05:30:00Z")
    }
    encoder := json.NewEncoder(os.Stdout)
    encoder.SetIndent("", "  ")
    if err := encoder.Encode(cases); err != nil { panic(err) }
}
