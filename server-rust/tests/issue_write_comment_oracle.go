// Standalone frozen-handler request structs: generate decoder expectations with
// Go's encoding/json, without importing the backend or accessing its database.
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type createRequest struct {
	Content          string   `json:"content"`
	Type             string   `json:"type"`
	ParentID         *string  `json:"parent_id"`
	AttachmentIDs    []string `json:"attachment_ids"`
	SuppressAgentIDs []string `json:"suppress_agent_ids"`
	SteerTaskIDs     []string `json:"steer_task_ids"`
	Steer            bool     `json:"steer"`
}

type editRequest struct {
	Content          string    `json:"content"`
	ContentBase      *string   `json:"content_base,omitempty"`
	AttachmentIDs    *[]string `json:"attachment_ids"`
	SuppressAgentIDs []string  `json:"suppress_agent_ids"`
	ExpectedRevision *int64    `json:"expected_revision,omitempty"`
}

type testCase struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Body       string `json:"-"`
	BodyBase64 string `json:"body_base64"`
	Decoded    any    `json:"decoded,omitempty"`
	Error      bool   `json:"error"`
}

func main() {
	cases := []testCase{
		{Name: "create-empty", Kind: "create", Body: `{}`},
		{Name: "create-null", Kind: "create", Body: `null`},
		{Name: "create-duplicate-null-scalar", Kind: "create", Body: `{"content":"first","content":null,"CONTENT":"last","CONTENT":null,"type":"progress_update","type":null,"steer":true,"STEER":null}`},
		{Name: "create-duplicate-pointer-and-list", Kind: "create", Body: `{"parent_id":"old","PARENT_ID":null,"attachment_ids":["a"],"attachment_ids":[null,"b"],"suppress_agent_ids":["a"],"SUPPRESS_AGENT_IDS":null}`},
		{Name: "create-folded-unicode-key", Kind: "create", Body: `{"ſteer":true,"Content":"x","tYPe":"comment"}`},
		{Name: "create-unknown-nested", Kind: "create", Body: `{"content":"x","unknown":{"content":9,"nested":[true,null]}}`},
		{Name: "create-trailing-value", Kind: "create", Body: `{"content":"x"} {"content":"ignored"}`},
		{Name: "create-trailing-garbage", Kind: "create", Body: `{"content":"x"} not-json`},
		{Name: "create-null-element", Kind: "create", Body: `{"content":"x","attachment_ids":[null]}`},
		{Name: "create-bad-type", Kind: "create", Body: `{"content":123}`},
		{Name: "create-bad-array-element", Kind: "create", Body: `{"attachment_ids":[1]}`},
		{Name: "create-bad-bool", Kind: "create", Body: `{"steer":"yes"}`},
		{Name: "create-array-root", Kind: "create", Body: `[]`},
		{Name: "create-invalid-json", Kind: "create", Body: `{"content":`},
		{Name: "edit-empty", Kind: "edit", Body: `{}`},
		{Name: "edit-null", Kind: "edit", Body: `null`},
		{Name: "edit-duplicate-null-scalar", Kind: "edit", Body: `{"content":"first","Content":null,"CONTENT":"last","CONTENT":null}`},
		{Name: "edit-pointer-clear", Kind: "edit", Body: `{"content":"x","content_base":"old","CONTENT_BASE":null,"expected_revision":5,"EXPECTED_REVISION":null,"attachment_ids":["a"],"Attachment_IDs":null}`},
		{Name: "edit-slice-null-element", Kind: "edit", Body: `{"content":"x","attachment_ids":[null,"b"],"suppress_agent_ids":[null]}`},
		{Name: "edit-fold-and-trailing", Kind: "edit", Body: `{"content":"x","Expected_Revision":2,"SUPPREſS_AGENT_IDS":["b"]} trailing`},
		{Name: "edit-bad-number", Kind: "edit", Body: `{"content":"x","expected_revision":1.2}`},
		{Name: "edit-bad-pointer", Kind: "edit", Body: `{"content":"x","content_base":true}`},
		{Name: "edit-string-root", Kind: "edit", Body: `"x"`},
		{Name: "create-slice-backing-reuse", Kind: "create", Body: `{"attachment_ids":["a","b","c"],"attachment_ids":["x"],"attachment_ids":[null,null,null,null]}`},
		{Name: "create-slice-empty-resets", Kind: "create", Body: `{"attachment_ids":["a"],"attachment_ids":[],"attachment_ids":[null]}`},
		{Name: "edit-slice-null-resets", Kind: "edit", Body: `{"attachment_ids":["a"],"attachment_ids":null,"attachment_ids":[null]}`},
		{Name: "edit-slice-reuse", Kind: "edit", Body: `{"attachment_ids":["a","b"],"attachment_ids":[null],"attachment_ids":[null,null]}`},
		{Name: "create-unpaired-high", Kind: "create", Body: `{"content":"\ud800"}`},
		{Name: "create-unpaired-low", Kind: "create", Body: `{"content":"\udc00"}`},
		{Name: "create-paired-surrogates", Kind: "create", Body: `{"content":"𐀀"}`},
		{Name: "create-repeated-high-surrogates", Kind: "create", Body: `{"content":"\ud800𐀀"}`},
		{Name: "edit-unpaired-base", Kind: "edit", Body: `{"content":"ok","content_base":"\uDFFF"}`},
		{Name: "create-invalid-utf8", Kind: "create", Body: `{"content":"` + string([]byte{0xff, 0xc0, 0x80}) + `"}`},
		{Name: "create-truncated-utf8", Kind: "create", Body: `{"content":"` + string([]byte{0xf0, 0x90}) + `"}`},
		{Name: "create-surrogate-utf8", Kind: "create", Body: `{"content":"` + string([]byte{0xed, 0xa0, 0x80}) + `"}`},
		{Name: "edit-valid-utf8", Kind: "edit", Body: `{"content":"café 😀","content_base":"a"}`},
		{Name: "create-escaped-backslash", Kind: "create", Body: `{"content":"\\ud800\\\""}`},
		{Name: "create-malformed-hex", Kind: "create", Body: `{"content":"\ud80z"}`},
		{Name: "create-raw-control", Kind: "create", Body: "{\"content\":\"x\ny\"}"},
		{Name: "create-invalid-outside-string", Kind: "create", Body: string([]byte{0xff}) + `{"content":"x"}`},
		{Name: "create-invalid-tail", Kind: "create", Body: `{"content":"x"}` + string([]byte{0xff})},
		{Name: "create-invalid-name", Kind: "create", Body: `{"con` + string([]byte{0xff}) + `tent":"x"}`},
	}
	for i := range cases {
		cases[i].BodyBase64 = base64.StdEncoding.EncodeToString([]byte(cases[i].Body))
		var value any
		if cases[i].Kind == "create" {
			value = &createRequest{}
		} else {
			value = &editRequest{}
		}
		err := json.NewDecoder(strings.NewReader(cases[i].Body)).Decode(value)
		cases[i].Error = err != nil
		if err == nil {
			if edit, ok := value.(*editRequest); ok {
				cases[i].Decoded = map[string]any{
					"content": edit.Content, "content_base": edit.ContentBase,
					"attachment_ids": edit.AttachmentIDs, "suppress_agent_ids": edit.SuppressAgentIDs,
					"expected_revision": edit.ExpectedRevision,
				}
			} else {
				cases[i].Decoded = value
			}
		}
	}
	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		panic(err)
	}
	if len(os.Args) != 2 {
		panic("usage: go run issue_write_comment_oracle.go output.json")
	}
	if err := os.WriteFile(os.Args[1], append(data, '\n'), 0644); err != nil {
		panic(err)
	}
	fmt.Printf("generated %d Go decoder cases\n", len(cases))
}
