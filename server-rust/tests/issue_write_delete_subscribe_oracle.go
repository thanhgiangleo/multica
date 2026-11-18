package main

import (
    "bytes"
    "encoding/base64"
    "encoding/json"
    "fmt"
)

type subscription struct {
    UserID *string `json:"user_id"`
    UserType *string `json:"user_type"`
}
type batch struct {
    IssueIDs []string `json:"issue_ids"`
}
type result struct {
    Kind string `json:"kind"`
    BodyBase64 string `json:"body_base64"`
    DecodeError bool `json:"decode_error"`
    UserID *string `json:"user_id,omitempty"`
    UserType *string `json:"user_type,omitempty"`
    IssueIDs []string `json:"issue_ids,omitempty"`
}

func main() {
    cases := [][]byte{
        []byte(`{"user_id":"one","USER_ID":"two"}`),
        []byte(`{"USER_ID":"two","user_id":null}`),
        []byte(`{"user_id":"one","USER_ID":7,"user_type":"agent"}`),
        []byte(`{"user_type":"agent","user_id":7}`),
        []byte(`{"user_id":7,"user_type":"agent"}`),
        []byte(`{"user_id":"one","user_id":{},"user_type":"agent"}`),
        []byte(`{"user_id":"one","USER_ID":7,"user_id":"two"}`),
        []byte(`{"user_id":"one","user_type":"agent"} trailing`),
        []byte(`{"user_id":"one", "user_type":"agent",`),
        []byte(`null`),
        []byte(`[]`),
        []byte(`{"user_id":"\ud800"}`),
        []byte(`{"user_id":"K","user_type":"ſ"}`),
        []byte("{\"user_id\":\"\xff\"}"),
        []byte(`{"user_id":"one","USER_type":null}`),
        []byte(`{"user_id":"one"}`),
        []byte(`{"user_id":{},"user_type":"agent"}`),
        []byte(`{"user_id":"one","USER_ID":null,"user_type":"member"}`),
    }
    for _, body := range cases {
        var v subscription
        err := json.NewDecoder(bytes.NewReader(body)).Decode(&v)
        r := result{Kind:"subscriber", BodyBase64:base64.StdEncoding.EncodeToString(body), DecodeError:err!=nil,UserID:v.UserID,UserType:v.UserType}
        raw, _ := json.Marshal(r)
        fmt.Println(string(raw))
    }
    batches := [][]byte{
        []byte(`{"issue_ids":["a","b"]}`),
        []byte(`{"ISSUE_IDS":["a"],"issue_ids":[null,"b"]}`),
        []byte(`{"issue_ids":null}`),
        []byte(`{"issue_ids":["a",5]}`),
        []byte(`{"issue_ids":["a"]} trailing`),
        []byte(`{"issue_ids":[]}`),
        []byte(`null`),
        []byte(`[]`),
        []byte(`{"issue_ids":["a"],"ISSUE_IDS":["c"]}`),
        []byte(`{"issue_ids":["a"],`),
        []byte(`{"issue_ids":["a"],"unknown":1e10000}`),
        []byte("{\"issue_ids\":[\"\xff\"]}"),
    }
    for _, body := range batches {
        var v batch
        err := json.NewDecoder(bytes.NewReader(body)).Decode(&v)
        r := result{Kind:"batch",BodyBase64:base64.StdEncoding.EncodeToString(body),DecodeError:err!=nil,IssueIDs:v.IssueIDs}
        raw, _ := json.Marshal(r)
        fmt.Println(string(raw))
    }
}
