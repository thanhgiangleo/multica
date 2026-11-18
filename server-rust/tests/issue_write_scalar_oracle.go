// Command issue_write_scalar_oracle generates frozen encoding/json decoder fixtures.
// Run: go run tests/issue_write_scalar_oracle.go tests/issue_write_scalar_oracle.json
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
)

type test struct {
	Name            string  `json:"name"`
	Kind            string  `json:"kind"`
	Body            string  `json:"body_base64"`
	Error           bool    `json:"error"`
	Decoded         *string `json:"decoded"`
	DecodedBase64   *string `json:"decoded_base64"`
	ValidationError string  `json:"validation_error,omitempty"`
}

func pointer(s string) *string { return &s }

// Frozen issue_metadata.go validateIssueMetadataValue, expressed as an error string.
func validationError(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "value is required"
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Sprintf("value must be valid JSON: %v", err)
	}
	switch value.(type) {
	case string, bool, float64:
		return ""
	case nil:
		return "value cannot be null (use DELETE to remove a key)"
	default:
		return "value must be a primitive: string, number, or bool"
	}
}

func main() {
	if len(os.Args) != 2 {
		panic("expected fixture output path")
	}
	cases := []struct {
		name, kind string
		body       []byte
	}{
		{"metadata missing", "metadata", []byte(`{}`)},
		{"metadata top null", "metadata", []byte(`null`)},
		{"metadata raw null", "metadata", []byte(`{"value":null}`)},
		{"metadata string", "metadata", []byte(`{"value":"ok"}`)},
		{"metadata string escapes", "metadata", []byte(`{"value":"A\\x"}`)},
		{"metadata negative zero", "metadata", []byte(`{"value":-0}`)},
		{"metadata decimal", "metadata", []byte(`{"value":2.00}`)},
		{"metadata exponent", "metadata", []byte(`{"value":1e+50}`)},
		{"metadata large exponent raw", "metadata", []byte(`{"value":1e400}`)},
		{"metadata nested overflow", "metadata", []byte(`{"value":{"n":1e400}}`)},
		{"metadata array overflow", "metadata", []byte(`{"value":[0,1e400]}`)},
		{"metadata numeric string", "metadata", []byte(`{"value":"1e400"}`)},
		{"metadata large integer", "metadata", []byte(`{"value":9007199254740993}`)},
		{"metadata boolean", "metadata", []byte(`{"value":true}`)},
		{"metadata array", "metadata", []byte(`{"value":[1,2]}`)},
		{"metadata nested", "metadata", []byte(`{"value":{"a":1,"a":2}}`)},
		{"metadata exact duplicate", "metadata", []byte(`{"value":1,"value":2}`)},
		{"metadata case duplicate", "metadata", []byte(`{"value":1,"VALUE":2}`)},
		{"metadata casefold", "metadata", []byte(`{"VaLuE":5}`)},
		{"metadata null then number", "metadata", []byte(`{"value":null,"value":5}`)},
		{"metadata number then null", "metadata", []byte(`{"value":5,"value":null}`)},
		{"metadata bad first duplicate", "metadata", []byte(`{"value":[],"value":5}`)},
		{"metadata unknown nested", "metadata", []byte(`{"ignored":{"a":1,"a":2},"value":2}`)},
		{"metadata first JSON only", "metadata", []byte(`{"value":8} trailing junk`)},
		{"metadata scalar", "metadata", []byte(`1`)},
		{"metadata empty", "metadata", []byte(``)},
		{"metadata malformed", "metadata", []byte(`{"value":`)},
		{"metadata surrogate raw", "metadata", []byte(`{"value":"\ud800"}`)},
		{"metadata unknown surrogate key", "metadata", []byte(`{"\ud800":0,"value":3}`)},
		{"metadata unknown invalid utf8 key", "metadata", []byte("{\"\xff\":0,\"value\":4}")},
		{"metadata raw invalid utf8", "metadata", []byte("{\"value\":\"\xff\xfe\"}")},
		{"metadata raw invalid nested utf8", "metadata", []byte("{\"value\":[\"\xff\"]}")},
		{"metadata invalid unknown bytes", "metadata", []byte("{\"ignored\":\"\xff\",\"VALUE\":true}")},
		{"metadata raw nested punctuation", "metadata", []byte(`{"value":{"a":["\\\"}",{"b":2}]} }`)},
		{"metadata escaped key", "metadata", []byte(`{"value":5}`)},
		{"metadata duplicate raw unicode", "metadata", []byte(`{"value":"\ud800","VALUE":"\udc00"}`)},
		{"metadata invalid discarded raw", "metadata", []byte("{\"value\":\"\xff\",\"VALUE\":7}")},
		{"metadata invalid raw syntax", "metadata", []byte("{\"value\":\"\xff\\x\"}")},
		{"property missing", "property", []byte(`{}`)},
		{"property top null", "property", []byte(`null`)},
		{"property null", "property", []byte(`{"value":null}`)},
		{"property duplicate", "property", []byte(`{"value":"first","VALUE":"last"}`)},
		{"property invalid raw utf8", "property", []byte("{\"value\":\"\xff\"}")},
		{"property nested raw", "property", []byte(`{"value":{"a":[1,{"b":true}]}}`)},
		{"property unknown invalid utf8 key", "property", []byte("{\"\xff\":0,\"value\":1}")},
		{"property surrogate key", "property", []byte(`{"\ud800":0,"value":1}`)},
		{"property trailing json", "property", []byte(`{"value":1} {"value":2}`)},
		{"property scalar invalid", "property", []byte(`[1]`)},
		{"property unknown overflow", "property", []byte(`{"ignored":1e400,"value":2}`)},
		{"property numeric overflow", "property", []byte(`{"value":1e400}`)},
		{"property invalid raw syntax", "property", []byte(`{"value":`)},
		{"reaction missing", "reaction", []byte(`{}`)},
		{"reaction top null", "reaction", []byte(`null`)},
		{"reaction string", "reaction", []byte(`{"emoji":"👍"}`)},
		{"reaction empty", "reaction", []byte(`{"emoji":""}`)},
		{"reaction null", "reaction", []byte(`{"emoji":null}`)},
		{"reaction casefold", "reaction", []byte(`{"EmOjI":"ok"}`)},
		{"reaction duplicate", "reaction", []byte(`{"emoji":"first","emoji":"last"}`)},
		{"reaction case duplicate", "reaction", []byte(`{"emoji":"first","EMOJI":"last"}`)},
		{"reaction duplicate null", "reaction", []byte(`{"emoji":"first","emoji":null}`)},
		{"reaction null then string", "reaction", []byte(`{"emoji":null,"emoji":"last"}`)},
		{"reaction bad first duplicate", "reaction", []byte(`{"emoji":1,"emoji":"last"}`)},
		{"reaction unknown nested", "reaction", []byte(`{"extra":{"x":1,"x":2},"emoji":"ok"}`)},
		{"reaction trailing junk", "reaction", []byte(`{"emoji":"ok"} nonsense`)},
		{"reaction malformed", "reaction", []byte(`{"emoji":`)},
		{"reaction scalar", "reaction", []byte(`[]`)},
		{"reaction escaped surrogate", "reaction", []byte(`{"emoji":"\ud800"}`)},
		{"reaction invalid utf8", "reaction", append([]byte(`{"emoji":"`), append([]byte{0xff}, []byte(`"}`)...)...)},
	}
	results := make([]test, 0, len(cases))
	for _, c := range cases {
		result := test{Name: c.name, Kind: c.kind, Body: base64.StdEncoding.EncodeToString(c.body)}
		decoder := json.NewDecoder(bytes.NewReader(c.body))
		if c.kind == "metadata" || c.kind == "property" {
			var request struct {
				Value json.RawMessage `json:"value"`
			}
			if err := decoder.Decode(&request); err != nil {
				result.Error = true
			} else {
				if request.Value != nil {
					result.Decoded = pointer(string(request.Value))
				}
				result.ValidationError = validationError(request.Value)
			}
		} else {
			var request struct {
				Emoji string `json:"emoji"`
			}
			if err := decoder.Decode(&request); err != nil {
				result.Error = true
			} else {
				result.Decoded = pointer(request.Emoji)
			}
		}
		if result.Decoded != nil {
			result.DecodedBase64 = pointer(base64.StdEncoding.EncodeToString([]byte(*result.Decoded)))
		}
		results = append(results, result)
	}
	buf, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(os.Args[1], append(buf, '\n'), 0644); err != nil {
		panic(err)
	}
}
