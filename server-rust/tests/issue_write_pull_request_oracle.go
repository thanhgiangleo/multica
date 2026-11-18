// Frozen handler normalizer extracted from pr_auto_complete.go:400-421.
// Standalone stdlib-only oracle: no database, credentials, providers or services.
package main

import (
    "encoding/json"
    "net/url"
    "os"
    "regexp"
    "strings"
)

var prURLPattern = regexp.MustCompile(`^(https?://[^/?#]+/.+?/(?:pull|pulls|merge_requests)/\d+)(?:[/?#].*)?$`)

func normalizePullRequestURL(raw string) (string, bool) {
    raw = strings.TrimSpace(raw)
    if raw == "" {
        return "", false
    }
    if !strings.Contains(raw, "://") {
        raw = "https://" + raw
    }
    u, err := url.Parse(raw)
    if err != nil || u.Host == "" {
        return "", false
    }
    m := prURLPattern.FindStringSubmatch(u.Scheme + "://" + u.Host + u.EscapedPath())
    if m == nil {
        return "", false
    }
    return strings.ToLower(strings.TrimRight(m[1], "/")), true
}

func main() {
    inputs := []string{
        "https://github.com/acme/widgets/pull/42",
        "  HTTPS://GitHub.com/Acme/Widgets/pull/42/files?diff=1#x  ",
        "github.com/acme/widgets/pull/42",
        "https://gitlab.com/grp/proj/-/merge_requests/7",
        "http://localhost:8888/o/r/pulls/8/",
        "https://u:p@example.com/o/r/pull/1",
        "https://example.com:443/o/r/pull/001",
        "https://example.com/o/r/../pull/1",
        "https://example.com/o/r/%2e%2e/pull/1",
        "https://example.com/o%2Fr/pull/1",
        "https://example.com/o/Việt Nam/pull/1",
        "https://example.com/o/Việt/pull/1",
        "https://example.com/o/a%20b/pull/1",
        "https://example.com/o/a%2fb c/pull/1",
        "https://example.com/o/[x]/pull/1",
        "https://example.com/o/x!$&'()*+,;=:@/pull/1",
        "https://example.com/o/r/pull/1/files/pull/2",
        "https://example.com/o/r/pull/1a/pull/2",
        "https://example.com/o/r/pull/1?bad=%zz",
        "https://example.com/o/r/pull/1#bad%zz",
        "https://example.com/o/%zz/pull/1",
        "https://example.com/o/r/pull/1\n",
        "https://example.com/o/r/pull/1\nx",
        "https://example.com/o/r/PULL/1",
        "https://example.com/o/r/pull/１２",
        "https://example.com/o/r/pull/1abc",
        "https://example.com/pull/1",
        "https://example.com//pull/1",
        "https://example.com///pull/1",
        "https://example.com/o//pull/1",
        "https://example.com/o/r/pull/-1",
        "https://example.com/o/r/pull/",
        "ftp://example.com/o/r/pull/1",
        "https://example.com:bad/o/r/pull/1",
        "https://exa_mple.com/o/r/pull/1",
        "https://[::1]:8080/o/r/pull/1",
        "https://例.example/o/r/pull/1",
        "https://example.com/o/r/issues/1",
        "",
    }
    rows := make([]map[string]any, 0, len(inputs))
    for _, raw := range inputs {
        normalized, ok := normalizePullRequestURL(raw)
        var result any
        if ok { result = normalized }
        rows = append(rows, map[string]any{"raw":raw, "normalized": result})
    }
    out, err := json.MarshalIndent(rows, "", "  ")
    if err != nil { panic(err) }
    if err := os.WriteFile(os.Args[1], append(out, '\n'), 0600); err != nil { panic(err) }
}
