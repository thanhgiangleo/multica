// Run with Go 1.23.4 to pin the frozen issueproperty URL contract.
package main

import (
    "fmt"
    "net/url"
    "os"
)

func main() {
    accepted := []string{
        "http://[fe80::1%25en0]/",
        "http://[fe80::1%25eth0]:99999/notes",
        "http://[fe80::1%25eth0%20wifi]/",
        "http://[::1%25]/",
        "http://[invalid%25eth0]/",
        "http://[]/",
    }
    rejected := []string{
        "http://[fe80::1%25eth0%ZZ]/",
        "http://[fe80::1%eth0]/",
        "http://[fe80::1%25eth0]:foo/",
        "http://[abc/",
        "http://[fe80::1%25a%00b]/",
    }
    for _, item := range accepted {
        parsed, err := url.Parse(item)
        if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
            fmt.Fprintf(os.Stderr, "expected accepted: %q: %v\n", item, err)
            os.Exit(1)
        }
    }
    for _, item := range rejected {
        parsed, err := url.Parse(item)
        if err == nil && parsed.Scheme == "http" && parsed.Host != "" {
            fmt.Fprintf(os.Stderr, "expected rejected: %q\n", item)
            os.Exit(1)
        }
    }
    fmt.Printf("Go bracket oracle: %d accepted, %d rejected\n", len(accepted), len(rejected))
}
