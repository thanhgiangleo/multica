// Standalone stdlib oracle copied from frozen handler/issue.go's create decoder.
// No server, credentials, database or HTTP calls.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type CreateIssueRequest struct {
	Title          string                     `json:"title"`
	Description    *string                    `json:"description"`
	Status         string                     `json:"status"`
	Priority       string                     `json:"priority"`
	AssigneeType   *string                    `json:"assignee_type"`
	AssigneeID     *string                    `json:"assignee_id"`
	ParentIssueID  *string                    `json:"parent_issue_id"`
	ProjectID      *string                    `json:"project_id"`
	Stage          *int32                     `json:"stage,omitempty"`
	StartDate      *string                    `json:"start_date"`
	DueDate        *string                    `json:"due_date"`
	AttachmentIDs  []string                   `json:"attachment_ids,omitempty"`
	LabelIDs       []string                   `json:"label_ids,omitempty"`
	Properties     map[string]json.RawMessage `json:"properties,omitempty"`
	OriginType     *string                    `json:"origin_type,omitempty"`
	OriginID       *string                    `json:"origin_id,omitempty"`
	AllowDuplicate bool                       `json:"allow_duplicate,omitempty"`
}

func (r *CreateIssueRequest) UnmarshalJSON(data []byte) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	type createIssueRequestAlias CreateIssueRequest
	var decoded createIssueRequestAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*r = CreateIssueRequest(decoded)
	return nil
}
func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := rejectDuplicateJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	return nil
}
func rejectDuplicateJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key must be a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := rejectDuplicateJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := rejectDuplicateJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("invalid JSON delimiter")
	}
}

type Case struct {
	Name       string `json:"name"`
	BodyBase64 string `json:"body_base64"`
	Error      bool   `json:"error"`
	Decoded    any    `json:"decoded,omitempty"`
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: go run issue_write_create_oracle.go <output.json>")
	}
	var cases []Case
	add := func(name string, body []byte) {
		c := Case{Name: name, BodyBase64: base64.StdEncoding.EncodeToString(body)}
		var req CreateIssueRequest
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&req); err != nil {
			c.Error = true
		} else {
			var props map[string]any
			if req.Properties != nil {
				props = make(map[string]any, len(req.Properties))
				for id, value := range req.Properties {
					var decoded any
					if err := json.Unmarshal(value, &decoded); err != nil {
						panic(err)
					}
					props[id] = decoded
				}
			}
			c.Decoded = map[string]any{
				"title": req.Title, "description": req.Description, "status": req.Status,
				"priority": req.Priority, "assignee_type": req.AssigneeType,
				"assignee_id": req.AssigneeID, "parent_issue_id": req.ParentIssueID,
				"project_id": req.ProjectID, "stage": req.Stage, "start_date": req.StartDate,
				"due_date": req.DueDate, "attachment_ids": append([]string{}, req.AttachmentIDs...),
				"label_ids": append([]string{}, req.LabelIDs...), "properties": props,
				"origin_type": req.OriginType, "origin_id": req.OriginID,
				"allow_duplicate": req.AllowDuplicate,
			}
		}
		cases = append(cases, c)
	}
	bodies := []string{
		`{}`, `null`, `[]`, `"hello"`, `false`,
		`{"title":"New"}`, `{"Title":"Folded"}`, `{"title":"First","TITLE":"Last"}`,
		`{"title":"First","title":"Last"}`,
		`{"title":"A","parent_issue_id":"old","PARENT_ISSUE_ID":null}`,
		`{"title":"First","TITLE":null}`, `{"title":null,"TITLE":"New"}`,
		`{"title":"A","unknown":{"x":1,"x":2}}`,
		`{"title":"A","properties":{"x":{"y":1,"y":2}}}`,
		`{"title":"A","properties":{"x":1},"PROPERTIES":{"y":2}}`,
		`{"title":"A","properties":{"x":1},"PROPERTIES":{"x":2}}`,
		`{"title":"A","properties":{"x":1},"PROPERTIES":null,"Properties":{"z":3}}`,
		`{"title":"A","properties":{"x":1,"x":2}}`,
		`{"title":"A","attachment_ids":["one","two"],"ATTACHMENT_IDS":[null,"new"]}`,
		`{"title":"A","label_ids":["one","two"],"LABEL_IDS":[null]}`,
		`{"title":"A","attachment_ids":["one"],"ATTACHMENT_IDS":null,"Attachment_IDs":[null]}`,
		`{"title":"A","label_ids":[null, "new"],"stage":3,"STAGE":null}`,
		`{"title":"A","allow_duplicate":true,"ALLOW_DUPLICATE":null}`,
		`{"title":"A","description":"old","DESCRIPTION":null,"STATUS":"TODO","priority":null}`,
		`{"title":"A","ASSIGNEE_TYPE":"member","assignee_id":"id","origin_type":"quick_create","ORIGIN_ID":"id"}`,
		`{"title":"A","start_date":"2026-11-03","due_date":"2026-11-04","stage":1}`,
		`{"title":"A","stage":2147483648}`, `{"title":1}`, `{"title":true}`,
		`{"title":"A","attachment_ids":[true]}`, `{"title":"A","properties":[]}`,
		`{"title":"A","unknown":[{"a":1},{"a":2}]}`,
		`{"title":"A","unknown":{"a":1e999}}`,
		`{"title":"\ud800"}`, `{"title":"A\u0000B"}`,
		`{"title":"First"}{"title":"Second"}`,
		`{"title":"First"} trailing`, `{"title":"First"} {"title":"Second","title":"Third"}`,
		`{"title":"A","properties":null}`, `{"title":"A","properties":{}}`,
		`{"title":"A","ſtatus":"done","Key":"unused"}`,
		`{"title":"A","unknown":{"a":"x:y,{[]}","b":[1,2]}}`,
		`{"title":"A","description":"one","DESCRIPTION":"two"}`,
		`{"title":"A","description":null,"DESCRIPTION":"two"}`,
		`{"title":"A","description":"one","DESCRIPTION":null}`,
		`{"title":"A","assignee_id":"one","ASSIGNEE_ID":"two"}`,
		`{"title":"A","assignee_id":null,"ASSIGNEE_ID":"two"}`,
		`{"title":"A","assignee_id":"one","ASSIGNEE_ID":null,"Assignee_Id":"three"}`,
		`{"title":"A","start_date":"2026-01-01","START_DATE":"2026-02-02"}`,
		`{"title":"A","stage":1,"STAGE":2}`, `{"title":"A","stage":null,"STAGE":2}`,
		`{"title":"A","stage":1,"STAGE":null}`,
		`{"title":"A","allow_duplicate":true,"ALLOW_DUPLICATE":false}`,
		`{"title":"A","Allow_Duplicate":true}`,
		`{"title":"A","attachment_ids":["one","two"],"ATTACHMENT_IDS":[null]}`,
		`{"title":"A","attachment_ids":[],"ATTACHMENT_IDS":[null]}`,
		`{"title":"A","properties":null,"Properties":{"a":1}}`,
		`{"title":"A","properties":{"a":1,"b":2},"Properties":null,"PROPERTIES":{"b":3}}`,
	}
	for i, body := range bodies {
		add(fmt.Sprintf("case-%02d", i), []byte(body))
	}
	for i, body := range [][]byte{
		[]byte(`{"title":"` + string([]byte{0xff}) + `"}`),
		[]byte(`{"title":"` + string([]byte{0xe2, 0x82}) + `"}`),
		[]byte{'{', '"', 't', 'i', 't', 'l', 'e', '"', ':', 0xff, '}'},
		[]byte(`{"title":"A"}` + string([]byte{0xff})),
	} {
		add(fmt.Sprintf("bytes-%02d", i), body)
	}
	out, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(os.Args[1], append(out, '\n'), 0644); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "generated %d frozen-Go create decoder cases\n", len(cases))
}
