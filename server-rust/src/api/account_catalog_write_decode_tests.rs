use super::*;

#[test]
fn workflow_request_null_fields_match_go_zero_values() {
    let body: WriteRequest = serde_json::from_value(json!({
        "name":null,"description":null,"dry_run":null,"status_mapping":null,
        "steps":[null,{"status_key":null,"instructions":null,"next_status_key":null,
        "back_status_key":null,"handler":{"type":null,"id":null},
        "new_status":{"name":null,"category":null,"color":null}}]
    }))
    .unwrap();
    assert!(body.name.is_none());
    assert!(body.description.is_none());
    assert!(!body.dry_run);
    assert!(body.status_mapping.is_empty());
    let steps = body.steps.unwrap();
    assert_eq!(steps.len(), 2);
    assert!(steps[0].status_key.is_empty());
    assert!(steps[1].instructions.is_empty());
    assert!(steps[1].handler.as_ref().unwrap().r#type.is_empty());
    assert!(steps[1].new_status.as_ref().unwrap().name.is_empty());
    let body: WriteRequest =
        serde_json::from_value(json!({"steps":null,"status_mapping":{"todo":null,"done":"done"}}))
            .unwrap();
    assert!(body.steps.is_none());
    assert_eq!(body.status_mapping["todo"], "");
    assert_eq!(body.status_mapping["done"], "done");
}

#[test]
fn workflow_request_wrong_non_null_types_stay_errors() {
    for body in [
        json!({"dry_run":"true"}),
        json!({"status_mapping":[]}),
        json!({"status_mapping":{"todo":3}}),
        json!({"steps":[1]}),
        json!({"steps":[{"instructions":false}]}),
        json!({"steps":[{"handler":{"id":4}}]}),
    ] {
        assert!(
            serde_json::from_value::<WriteRequest>(body.clone()).is_err(),
            "{body}"
        );
    }
}
