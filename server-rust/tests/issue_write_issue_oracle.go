// Standalone stdlib oracle for frozen handler/issue.go UpdateIssueRequest and
// BatchUpdateIssuesRequest. No server, credentials, database or HTTP calls.
package main

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "os"
)

type UpdateIssueRequest struct {
    ExpectedRevision *int64 `json:"expected_revision"`
    Title *string `json:"title"`
    TitleBase *string `json:"title_base"`
    Description *string `json:"description"`
    DescriptionBase *string `json:"description_base"`
    Status *string `json:"status"`
    Priority *string `json:"priority"`
    AssigneeType *string `json:"assignee_type"`
    AssigneeID *string `json:"assignee_id"`
    Position *float64 `json:"position"`
    StartDate *string `json:"start_date"`
    DueDate *string `json:"due_date"`
    ParentIssueID *string `json:"parent_issue_id"`
    ProjectID *string `json:"project_id"`
    Stage *int32 `json:"stage"`
    AttachmentIDs []string `json:"attachment_ids"`
    SuppressRun bool `json:"suppress_run"`
    HandoffNote string `json:"handoff_note"`
    StopPreviousAssigneeRuns bool `json:"stop_previous_assignee_runs"`
    EnterWorkflowStep bool `json:"enter_workflow_step"`
    DuplicateOfIssueID *string `json:"duplicate_of_issue_id"`
}
type BatchUpdateIssuesRequest struct {
    IssueIDs []string `json:"issue_ids"`
    Updates UpdateIssueRequest `json:"updates"`
}
type Case struct {
    Name string `json:"name"`
    Kind string `json:"kind"`
    BodyBase64 string `json:"body_base64"`
    Error bool `json:"error"`
    Decoded any `json:"decoded,omitempty"`
}

func canonicalRaw(raw map[string]json.RawMessage) map[string]any {
    if raw == nil { return nil }
    out := make(map[string]any,len(raw))
    for key, value := range raw {
        var decoded any
        if err := json.Unmarshal(value,&decoded); err != nil { panic(err) }
        out[key] = decoded
    }
    return out
}

func main() {
    if len(os.Args) != 2 { panic("usage: go run issue_write_issue_oracle.go <output.json>") }
    var cases []Case
    add := func(name, kind string, body []byte) {
        c := Case{Name:name, Kind:kind, BodyBase64:base64.StdEncoding.EncodeToString(body)}
        var raw map[string]json.RawMessage
        if kind == "update" {
            var req UpdateIssueRequest
            if json.Unmarshal(body, &req) == nil && json.Unmarshal(body, &raw) == nil {
                c.Decoded = map[string]any{"request":req,"raw":canonicalRaw(raw)}
            } else { c.Error = true }
        } else {
            var req BatchUpdateIssuesRequest
            if json.Unmarshal(body, &req) == nil && json.Unmarshal(body, &raw) == nil {
                var updates map[string]json.RawMessage
                if value, present := raw["updates"]; present { _ = json.Unmarshal(value, &updates) }
                c.Decoded = map[string]any{"request":req,"raw":canonicalRaw(updates)}
            } else { c.Error = true }
        }
        cases = append(cases,c)
    }
    updates := []string{
        `{}`, `null`, `[]`, `"string"`, `{ "title":"First", "TITLE":null }`,
        `{ "TITLE":"First", "title":"Second" }`,
        `{ "title":"First", "TITLE":"Second", "title":null }`,
        `{ "status":false }`, `{ "position":0.25, "POSITION":null }`,
        `{ "expected_revision":7, "EXPECTED_REVISION":null }`,
        `{ "expected_revision":7, "Expected_Revision":8 }`,
        `{ "attachment_ids":["one","two"],"ATTACHMENT_IDS":[null,"new"] }`,
        `{ "attachment_ids":["one","two"],"attachment_ids":[null] }`,
        `{ "attachment_ids":["one"],"attachment_ids":null,"attachment_ids":[null] }`,
        `{ "attachment_ids":[], "attachment_ids":[null] }`,
        `{ "attachment_ids":["one"], "attachment_ids":[false] }`,
        `{ "suppress_run":true, "SUPPRESS_RUN":null }`,
        `{ "handoff_note":"go", "HANDOFF_NOTE":null }`,
        `{ "duplicate_of_issue_id":"a", "DUPLICATE_OF_ISSUE_ID":"b" }`,
        `{ "duplicate_of_issue_id":null, "DUPLICATE_OF_ISSUE_ID":"b" }`,
        `{ "parent_issue_id":"a", "PARENT_ISSUE_ID":null, "stage":3, "STAGE":null }`,
        `{ "attachment_ids":[null, "b"], "stage":2147483648 }`,
        `{ "unknown": [false, {"title":"anything"}], "title":"safe" }`,
        `{ "title":"a" } { "title":"b" }`,
        `{ "title":"a" } trailing`,
        `{ "ſtatus":"todo", "ſuppress_run":true, "Key":"unused" }`,
        `{ "title":"\ud800", "description":"\udfff" }`,
        `{ "title":"a\u0000b" }`,
        `{ "title":1 }`, `{ "attachment_ids":"nope" }`,
    }
    for index, body := range updates { add(fmt.Sprintf("update-%02d", index),"update",[]byte(body)) }
    for index, body := range [][]byte{
        []byte(`{"title":"`+string([]byte{0xff})+`"}`),
        []byte(`{"title":"`+string([]byte{0xe2,0x82})+`"}`),
        []byte(`{"title":"`+string([]byte{0xc0,0xaf})+`"}`),
        []byte{'{','"','t','i','t','l','e','"',':',0xff,'}'},
    } { add(fmt.Sprintf("update-bytes-%02d",index),"update",body) }
    batches := []string{
        `{}`, `null`, `[]`, `{"issue_ids":["x",null,"y"],"updates":{"status":"todo"}}`,
        `{ "issue_ids":["one","two"], "ISSUE_IDS":[null] }`,
        `{ "issue_ids":["one"], "issue_ids":null, "issue_ids":[null] }`,
        `{ "updates":{"title":"old","attachment_ids":["one","two"]}, "UPDATES":{"status":"todo","attachment_ids":[null,"new"]} }`,
        `{ "updates":{"title":"old","suppress_run":true}, "updates":{"description":"new","SUPPRESS_RUN":null} }`,
        `{ "updates":{"title":"old"}, "UPDATES":null }`,
        `{ "updates":null, "UPDATES":{"description":"new"} }`,
        `{ "updates":{"title":"old"}, "updates":false }`,
        `{ "updates":{"title":2} }`,
        `{ "issue_ids":[], "updates":{"attachment_ids":[null]} }`,
        `{ "issue_ids":["a"], "updates":{"parent_issue_id":"first", "PARENT_ISSUE_ID":null} }`,
        `{ "issue_ids":["a"], "updates":{"status":"todo"}, "update":{"status":"done"} }`,
        `{ "issue_ids":["a"] } { "issue_ids":["b"] }`,
        `{ "ISSUE_IDS":["a"], "UPDATES":{"DUPLICATE_OF_ISSUE_ID":"b"} }`,
    }
    for index, body := range batches { add(fmt.Sprintf("batch-%02d", index),"batch",[]byte(body)) }
    encoded,err:=json.MarshalIndent(cases,"","  ")
    if err!=nil { panic(err) }
    if err=os.WriteFile(os.Args[1],append(encoded,'\n'),0644);err!=nil {panic(err)}
    fmt.Fprintf(os.Stderr,"generated %d frozen-Go issue decoder cases\n",len(cases))
}
