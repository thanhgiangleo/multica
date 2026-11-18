// Run with Go 1.23.4 to pin frozen issueproperty path/fragment escape behavior.
package main

import (
    "fmt"
    "net/url"
)

func main() {
    cases := []struct { input string; want bool }{
        {"http://example.com/path%ZZ", false},
        {"http://example.com/path%", false},
        {"http://example.com/#%ZZ", false},
        {"http://example.com/?q=%ZZ", true},
        {"http://example.com/%2F#%25", true},
        {"http://example.com/?q=%ZZ#%", false},
    }
    for _, test := range cases {
        parsed, err := url.Parse(test.input)
        allowed := err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
        if allowed != test.want { panic(fmt.Sprintf("%q allowed=%t, want=%t", test.input, allowed, test.want)) }
    }
    fmt.Printf("Go URL escape oracle: %d cases matched\n", len(cases))
}
