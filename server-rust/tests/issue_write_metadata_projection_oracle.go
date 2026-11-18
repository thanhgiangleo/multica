// Reproduce frozen Go util.JSONObjectOrEmpty's response projection, not storage.
// Run: go run tests/issue_write_metadata_projection_oracle.go
package main

import (
	"encoding/json"
	"os"
)

type projectionCase struct {
	Raw      string         `json:"raw"`
	Expected map[string]any `json:"expected"`
}

func main() {
	inputs := []string{
		`{"count":9007199254740993}`,
		`{"count":9007199254740994}`,
		`{"count":-9007199254740993}`,
		`{"count":18446744073709551617}`,
		`{"count":9223372036854775807}`,
		`{"count":0.1234567890123456789}`,
		`{"count":1e-300}`,
		`{"count":1e300}`,
		`{"count":-0.0}`,
		`{"nested":{"items":[9007199254740993,true,"text",null]}}`,
		`null`,
		`[1,2]`,
	}
	cases := make([]projectionCase, 0, len(inputs))
	for _, raw := range inputs {
		var out map[string]any
		if err := json.Unmarshal([]byte(raw), &out); err != nil || out == nil {
			out = map[string]any{}
		}
		cases = append(cases, projectionCase{Raw: raw, Expected: out})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cases); err != nil {
		panic(err)
	}
}
