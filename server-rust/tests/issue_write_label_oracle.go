// Command issue_write_label_oracle generates frozen Go label decoder fixtures.
// Run: GO111MODULE=off go run tests/issue_write_label_oracle.go tests/issue_write_label_oracle.json
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
)

type result struct {
	Name  string `json:"name"`
	Body  string `json:"body_base64"`
	Error bool   `json:"error"`
	Label string `json:"label_id"`
}

func main() {
	if len(os.Args) != 2 {
		panic("expected fixture output path")
	}
	cases := []struct {
		name string
		body []byte
	}{
		{"missing", []byte(`{}`)},
		{"top null", []byte(`null`)},
		{"string", []byte(`{"label_id":"first"}`)},
		{"casefold", []byte(`{"LaBeL_iD":"first"}`)},
		{"unicode fold", []byte(`{"label_İd":"x","LABEL_ID":"last"}`)},
		{"duplicate", []byte(`{"label_id":"first","label_id":"last"}`)},
		{"duplicate null", []byte(`{"label_id":"first","LABEL_ID":null}`)},
		{"null then string", []byte(`{"label_id":null,"LABEL_ID":"last"}`)},
		{"bad before duplicate", []byte(`{"label_id":false,"label_id":"last"}`)},
		{"bad after duplicate", []byte(`{"label_id":"first","label_id":false}`)},
		{"unknown object", []byte(`{"ignored":{"a":1,"a":2},"label_id":"last"}`)},
		{"unknown exponent", []byte(`{"ignored":1e400,"label_id":"last"}`)},
		{"trailing garbage", []byte(`{"label_id":"last"} garbage`)},
		{"scalar", []byte(`[]`)},
		{"scalar string", []byte(`"hello"`)},
		{"malformed", []byte(`{"label_id":`)},
		{"unknown invalid utf8", []byte("{\"ignored\":\"\xff\",\"label_id\":\"last\"}")},
		{"value invalid utf8", []byte("{\"label_id\":\"\xff\"}")},
		{"unpaired surrogate", []byte(`{"label_id":"\ud800"}`)},
	}
	out := make([]result, 0, len(cases))
	for _, c := range cases {
		var req struct {
			LabelID string `json:"label_id"`
		}
		err := json.NewDecoder(bytes.NewReader(c.body)).Decode(&req)
		out = append(out, result{c.name, base64.StdEncoding.EncodeToString(c.body), err != nil, req.LabelID})
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(os.Args[1], append(data, '\n'), 0644); err != nil {
		panic(err)
	}
}
