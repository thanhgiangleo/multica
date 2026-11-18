//! Issue list transport, filtering and isolation against owned PostgreSQL 16.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-sprint-issue-list";
fn token(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
fn app(pool: Arc<Pool>) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-list".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    )
}
async fn call(
    app: &axum::Router,
    ws: Uuid,
    method: &str,
    query: &str,
    body: &str,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(format!(
        "/api/issues/{}?workspace_id={ws}&{query}",
        if method == "POST" { "query" } else { "" }
    ));
    if let Some(auth) = auth {
        req = req.header("authorization", format!("Bearer {auth}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| panic!("non-JSON response ({status}): {bytes:?}")),
    )
}
async fn grouped_call(
    app: &axum::Router,
    ws: Uuid,
    query: &str,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!("/api/issues/grouped?workspace_id={ws}&{query}"));
    if let Some(auth) = auth {
        req = req.header("authorization", format!("Bearer {auth}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn ids(body: &Value) -> Vec<String> {
    body["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_owned())
        .collect()
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn issue_list_scope_transport_filters_and_projection() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('List user',$1) RETURNING id",
            &[&format!("list-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('List outsider',$1) RETURNING id",
            &[&format!("list-outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('List A',$1,'LST') RETURNING id",
            &[&format!("queries-list-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('List B',$1,'EXT') RETURNING id",
            &[&format!("queries-list-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,'shipped','Shipped','done','#222222')",&[&ws]).await.unwrap();
    let property:Uuid=db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Score','number') RETURNING id",&[&ws]).await.unwrap().get(0);
    let label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'List label','#123456','issue') RETURNING id",&[&ws]).await.unwrap().get(0);
    let insert = "INSERT INTO issue(workspace_id,number,title,status,priority,creator_type,creator_id,assignee_type,assignee_id,metadata,properties,position) VALUES ($1,$2,$3,$4,$5,'member',$6,'member',$6,$7,$8,$9) RETURNING id";
    let mut issues = Vec::new();
    for (num, title, status, priority, pos) in [
        (1, "alpha_ first", "todo", "high", 1f64),
        (2, "alpha% second", "in_progress", "low", 2f64),
        (3, "alpha third", "shipped", "medium", 3f64),
        (4, "alpha fourth", "cancelled", "none", 4f64),
    ] {
        let meta = json!({"region":"east"});
        let properties = json!({property.to_string():num});
        let id: Uuid = db
            .query_one(
                insert,
                &[
                    &ws,
                    &num,
                    &title,
                    &status,
                    &priority,
                    &user,
                    &meta,
                    &properties,
                    &pos,
                ],
            )
            .await
            .unwrap()
            .get(0);
        issues.push(id);
    }
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&issues[0], &label],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=$2",
        &[&issues[0], &issues[3]],
    )
    .await
    .unwrap();
    let foreign: Uuid = db
        .query_one(
            insert,
            &[
                &other,
                &1i32,
                &"FOREIGNSECRET",
                &"todo",
                &"high",
                &user,
                &json!({"region":"east"}),
                &json!({property.to_string():1}),
                &0f64,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let app = app(pool.clone());
    let auth = token(user);
    assert_eq!(
        call(&app, ws, "GET", "", "", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, ws, "GET", "", "", Some(&token(outsider)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, all) = call(&app, ws, "GET", "sort=position", "", Some(&auth)).await;
    assert_eq!(status, StatusCode::OK, "{all}");
    assert_eq!(all["total"], 4);
    assert_eq!(
        ids(&all),
        issues.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    assert!(!ids(&all).contains(&foreign.to_string()));
    assert_eq!(all["issues"][0]["labels"][0]["usage_count"], 0);
    assert_eq!(all["issues"][0]["labels"][0]["description"], "");
    assert!(all["issues"][0].get("search_updated_at").is_none());
    assert_eq!(
        all["issues"][3]["duplicate_of"]["id"],
        issues[0].to_string()
    );
    assert_eq!(
        grouped_call(&app, ws, "", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        grouped_call(&app, ws, "", Some(&token(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        grouped_call(&app, ws, "group_by=status", Some(&auth))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        grouped_call(&app, ws, "group_assignee_type=agent", Some(&auth))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, grouped) =
        grouped_call(&app, ws, "sort=position&limit=2&offset=1", Some(&auth)).await;
    assert_eq!(status, StatusCode::OK, "{grouped}");
    assert_eq!(grouped["groups"].as_array().unwrap().len(), 1);
    assert_eq!(
        grouped["groups"][0]["id"],
        format!("assignee:member:{user}")
    );
    assert_eq!(grouped["groups"][0]["total"], 4);
    assert_eq!(
        grouped["groups"][0]["issues"][0]["id"],
        issues[1].to_string()
    );
    assert_eq!(
        grouped["groups"][0]["issues"][1]["id"],
        issues[2].to_string()
    );
    assert_eq!(
        grouped_call(&app, ws, "limit=1&offset=4", Some(&auth))
            .await
            .1["groups"],
        json!([])
    );
    let (_, page) = call(
        &app,
        ws,
        "GET",
        "limit=1&offset=1&sort=position",
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(page["total"], 4);
    assert_eq!(ids(&page), vec![issues[1].to_string()]);
    let (_, post) = call(
        &app,
        ws,
        "POST",
        "",
        r#"{"limit":"1","offset":"1","sort":"position"}"#,
        Some(&auth),
    )
    .await;
    assert_eq!(page, post);
    let (_, open) = call(
        &app,
        ws,
        "GET",
        "open_only=true&status=done&limit=1&sort=title",
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(open["total"], 2);
    assert_eq!(
        ids(&open),
        vec![issues[0].to_string(), issues[1].to_string()]
    );
    for (filter, expected) in [
        ("status_category=done", vec![issues[2]]),
        ("q=alpha%25", vec![issues[1]]),
        ("q=alpha_", vec![issues[0]]),
        ("ids=", vec![]),
    ] {
        let (status, body) = call(&app, ws, "GET", filter, "", Some(&auth)).await;
        assert_eq!(status, StatusCode::OK, "{filter}: {body}");
        assert_eq!(
            ids(&body),
            expected.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "{filter}"
        );
    }
    let (_, by_label) = call(
        &app,
        ws,
        "GET",
        &format!("label_ids={label}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(ids(&by_label), vec![issues[0].to_string()]);
    let (_, by_meta) = call(
        &app,
        ws,
        "POST",
        "",
        r#"{"metadata":"{\"region\":\"east\"}","limit":"2"}"#,
        Some(&auth),
    )
    .await;
    assert_eq!(by_meta["total"], 4);
    let (_, by_property) = call(
        &app,
        ws,
        "POST",
        "",
        &json!({"properties":json!({property.to_string():["2"]}).to_string()}).to_string(),
        Some(&auth),
    )
    .await;
    assert_eq!(ids(&by_property), vec![issues[1].to_string()]);
    let (_,numeric)=call(&app,ws,"POST","",&json!({"properties":json!({property.to_string():[{"op":"gt","value":"2"}]}).to_string(),"sort":format!("property:{property}"),"direction":"desc"}).to_string(),Some(&auth)).await;
    assert_eq!(
        ids(&numeric),
        vec![issues[3].to_string(), issues[2].to_string()]
    );
    let (_, status_sort) = call(&app, ws, "GET", "sort=status", "", Some(&auth)).await;
    assert_eq!(
        ids(&status_sort),
        issues.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    let (_, none) = call(
        &app,
        ws,
        "GET",
        &format!("involves_user_id={user}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(none["total"], 0);
    assert_eq!(
        call(
            &app,
            ws,
            "GET",
            "open_only=true&date_field=created_at&date_start=no",
            "",
            Some(&auth)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (_, empty_post) = call(&app, ws, "POST", "", "null", Some(&auth)).await;
    assert_eq!(empty_post["total"], 4);
    for bad_query in [
        "assignee_ids=invalid",
        "properties=%7Bbad",
        "date_field=created_at&date_start=no",
        "sort=property:bad",
        "assignee_types=bad",
    ] {
        let (status, body) = call(&app, ws, "GET", bad_query, "", Some(&auth)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad_query}: {body}");
    }
    for bad_body in ["", "not-json", r#"{"limit":1}"#] {
        let (status, body) = call(&app, ws, "POST", "", bad_body, Some(&auth)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad_body}: {body}");
        assert_eq!(body["error"], "invalid request body");
    }

    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'List synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'List other synthetic','cloud','synthetic','online') RETURNING id", &[&other]).await.unwrap().get(0);
    let owned_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id) VALUES ($1,'List owned agent','cloud',$2,$3) RETURNING id", &[&ws,&runtime,&user]).await.unwrap().get(0);
    let unowned_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id) VALUES ($1,'List unowned agent','cloud',$2,$3) RETURNING id", &[&ws,&runtime,&outsider]).await.unwrap().get(0);
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id) VALUES ($1,'List foreign agent','cloud',$2,$3) RETURNING id", &[&other,&foreign_runtime,&user]).await.unwrap().get(0);
    let member_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'List member squad',$2,$3) RETURNING id", &[&ws,&unowned_agent,&user]).await.unwrap().get(0);
    let leader_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'List leader squad',$2,$3) RETURNING id", &[&ws,&owned_agent,&user]).await.unwrap().get(0);
    let agent_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'List agent squad',$2,$3) RETURNING id", &[&ws,&unowned_agent,&user]).await.unwrap().get(0);
    let foreign_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'List foreign squad',$2,$3) RETURNING id", &[&other,&foreign_agent,&user]).await.unwrap().get(0);
    db.execute("INSERT INTO squad_member(squad_id,member_type,member_id) VALUES ($1,'member',$2),($3,'agent',$4)", &[&member_squad,&user,&agent_squad,&owned_agent]).await.unwrap();
    db.execute(
        "UPDATE issue SET assignee_type='agent',assignee_id=$1 WHERE id=$2",
        &[&owned_agent, &issues[0]],
    )
    .await
    .unwrap();
    for (issue, assignee) in [
        (issues[1], member_squad),
        (issues[2], leader_squad),
        (issues[3], agent_squad),
    ] {
        db.execute(
            "UPDATE issue SET assignee_type='squad',assignee_id=$1 WHERE id=$2",
            &[&assignee, &issue],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE issue SET assignee_type='squad',assignee_id=$1 WHERE id=$2",
        &[&foreign_squad, &foreign],
    )
    .await
    .unwrap();
    let (status, involved) = call(
        &app,
        ws,
        "GET",
        &format!("involves_user_id={user}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{involved}");
    assert_eq!(
        ids(&involved),
        issues.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    assert!(!ids(&involved).contains(&foreign.to_string()));
    let (status, grouped) = grouped_call(
        &app,
        ws,
        &format!("involves_user_id={user}&limit=1"),
        Some(&auth),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{grouped}");
    let groups = grouped["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 4);
    assert_eq!(groups[0]["assignee_type"], "agent");
    let mut squads = vec![member_squad, leader_squad, agent_squad];
    squads.sort();
    for (group, squad) in groups[1..].iter().zip(squads) {
        assert_eq!(group["assignee_id"], squad.to_string());
        assert_eq!(group["total"], 1);
        assert_eq!(group["issues"].as_array().unwrap().len(), 1);
    }
    assert_eq!(groups[0]["issues"][0]["labels"][0]["id"], label.to_string());
    let (_, one_group) = grouped_call(
        &app,
        ws,
        &format!("group_assignee_type=squad&group_assignee_id={member_squad}"),
        Some(&auth),
    )
    .await;
    assert_eq!(one_group["groups"].as_array().unwrap().len(), 1);
    assert_eq!(
        one_group["groups"][0]["issues"][0]["id"],
        issues[1].to_string()
    );
    assert_eq!(
        grouped_call(&app, ws, "status_category=done", Some(&auth))
            .await
            .1["groups"][0]["issues"][0]["id"],
        issues[2].to_string()
    );
    assert_eq!(
        grouped_call(&app, other, "status_category=done", Some(&auth))
            .await
            .1["groups"],
        json!([])
    );
    let (_, no_direct) = call(
        &app,
        ws,
        "GET",
        &format!("involves_user_id={outsider}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(no_direct["total"], 2);
    assert_eq!(
        ids(&no_direct),
        vec![issues[1].to_string(), issues[3].to_string()]
    );
    let (_, actors) = call(&app, ws, "GET", &format!("assignee_filters=agent:{owned_agent},squad:{member_squad}&creator_filters=member:{user}"), "", Some(&auth)).await;
    assert_eq!(
        ids(&actors),
        vec![issues[0].to_string(), issues[1].to_string()]
    );
    let (_, categories) = call(&app, ws, "GET", "status_category=done", "", Some(&auth)).await;
    assert_eq!(ids(&categories), vec![issues[2].to_string()]);
    let (_, no_category_leak) =
        call(&app, other, "GET", "status_category=done", "", Some(&auth)).await;
    assert_eq!(no_category_leak["total"], 0);
    let (_, no_property_leak) = call(
        &app,
        other,
        "GET",
        &format!("sort=property:{property}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(ids(&no_property_leak), vec![foreign.to_string()]);
    let (_, never_match) = call(
        &app,
        ws,
        "GET",
        &format!("label_ids={label}&status=cancelled"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(never_match["total"], 0);

    // Cross-workspace assignee pointers must not satisfy indirect ownership.
    for (kind, assignee) in [("agent", foreign_agent), ("squad", foreign_squad)] {
        db.execute(
            "UPDATE issue SET assignee_type=$1,assignee_id=$2 WHERE id=$3",
            &[&kind, &assignee, &issues[3]],
        )
        .await
        .unwrap();
        let (status, body) = call(
            &app,
            ws,
            "GET",
            &format!("involves_user_id={user}"),
            "",
            Some(&auth),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            ids(&body),
            issues[..3]
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        );
    }
    db.execute(
        "UPDATE issue SET assignee_type=NULL,assignee_id=NULL,project_id=NULL WHERE id=$1",
        &[&issues[3]],
    )
    .await
    .unwrap();
    let (_, unassigned) = call(&app, ws, "GET", "include_no_assignee=true", "", Some(&auth)).await;
    assert_eq!(ids(&unassigned), vec![issues[3].to_string()]);
    let (_, or_unassigned) = call(
        &app,
        ws,
        "GET",
        &format!("assignee_filters=agent:{owned_agent}&include_no_assignee=true"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(
        ids(&or_unassigned),
        vec![issues[0].to_string(), issues[3].to_string()]
    );

    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'List project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE issue SET project_id=$1 WHERE id=$2",
        &[&project, &issues[0]],
    )
    .await
    .unwrap();
    let (_, project_only) = call(
        &app,
        ws,
        "GET",
        &format!("project_ids={project}"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(ids(&project_only), vec![issues[0].to_string()]);
    let (_, no_project) = call(&app, ws, "GET", "include_no_project=true", "", Some(&auth)).await;
    assert_eq!(
        ids(&no_project),
        issues[1..]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    let (_, number) = call(&app, ws, "GET", "q=%2B1", "", Some(&auth)).await;
    assert_eq!(ids(&number), vec![issues[0].to_string()]);

    // Half-open UTC date windows and tied timestamps retain stable pagination.
    db.execute("UPDATE issue SET created_at='2026-10-01T00:00:00Z',updated_at='2026-10-01T00:00:00Z',start_date=NULL,due_date=NULL WHERE workspace_id=$1", &[&ws]).await.unwrap();
    db.execute(
        "UPDATE issue SET created_at='2026-10-02T00:00:00Z',start_date='2026-11-01' WHERE id=$1",
        &[&issues[1]],
    )
    .await
    .unwrap();
    let params = json!({"date_field":"created_at","date_start":"2026-10-01T00:00:00Z","date_end":"2026-10-02T00:00:00Z"});
    let (status, window) = call(&app, ws, "POST", "", &params.to_string(), Some(&auth)).await;
    assert_eq!(status, StatusCode::OK, "{window}");
    assert_eq!(
        ids(&window),
        vec![
            issues[0].to_string(),
            issues[2].to_string(),
            issues[3].to_string()
        ]
    );
    let (_, scheduled) = call(&app, ws, "GET", "scheduled=true", "", Some(&auth)).await;
    assert_eq!(ids(&scheduled), vec![issues[1].to_string()]);
    let mut expected = vec![issues[0], issues[2], issues[3]];
    expected.sort_by(|a, b| b.cmp(a));
    expected.insert(0, issues[1]);
    let mut pages = Vec::new();
    for offset in 0..4 {
        let (status, page) = call(
            &app,
            ws,
            "GET",
            &format!("sort=created_at&direction=desc&limit=1&offset={offset}"),
            "",
            Some(&auth),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page["total"], 4);
        pages.extend(ids(&page));
    }
    assert_eq!(
        pages,
        expected.iter().map(ToString::to_string).collect::<Vec<_>>()
    );

    // Corrupt/missing property values must not break numeric ordering or casts.
    db.execute(
        "UPDATE issue SET properties=$1 WHERE id=$2",
        &[&json!({property.to_string():"not-a-number"}), &issues[0]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue SET properties='{}'::jsonb WHERE id=$1",
        &[&issues[3]],
    )
    .await
    .unwrap();
    let (_, sorted) = call(
        &app,
        ws,
        "GET",
        &format!("sort=property:{property}&direction=desc"),
        "",
        Some(&auth),
    )
    .await;
    assert_eq!(
        &ids(&sorted)[..2],
        &[issues[2].to_string(), issues[1].to_string()]
    );
    for (alternatives, expected) in [
        (json!(["__none__"]), vec![issues[3]]),
        (
            json!([{"op":"gte","value":"2"}]),
            vec![issues[1], issues[2]],
        ),
        (
            json!([{"op":"contains","value":"not-a-number"}]),
            vec![issues[0]],
        ),
    ] {
        let body = json!({"properties":json!({property.to_string():alternatives}).to_string()});
        let (status, result) = call(&app, ws, "POST", "", &body.to_string(), Some(&auth)).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(
            ids(&result),
            expected.iter().map(ToString::to_string).collect::<Vec<_>>()
        );
    }
    for query in [
        "assignee_filters=agent:bad",
        "creator_filters=invalid",
        "project_ids=bad",
        "label_ids=bad",
        "involves_user_id=bad",
        "sort=title&direction=sideways",
        "date_field=created_at&date_start=2026-10-02T00:00:00Z&date_end=2026-10-01T00:00:00Z",
    ] {
        let (status, body) = call(&app, ws, "GET", query, "", Some(&auth)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        let (status, body) = grouped_call(&app, ws, query, Some(&auth)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "grouped {query}: {body}");
    }

    // Per-group offsets apply independently, with stable UUID tie breaks.
    db.execute("UPDATE issue SET position=1,created_at='2026-10-01T00:00:00Z',assignee_type=NULL,assignee_id=NULL WHERE workspace_id=$1", &[&ws]).await.unwrap();
    db.execute(
        "UPDATE issue SET assignee_type='member',assignee_id=$1 WHERE id=ANY($2::uuid[])",
        &[&user, &issues[..2].to_vec()],
    )
    .await
    .unwrap();
    let mut member_ids = issues[..2].to_vec();
    member_ids.sort_by(|a, b| b.cmp(a));
    let mut unassigned_ids = issues[2..].to_vec();
    unassigned_ids.sort_by(|a, b| b.cmp(a));
    for offset in 0..2 {
        let (status, body) = grouped_call(
            &app,
            ws,
            &format!("limit=1&offset={offset}&sort=position"),
            Some(&auth),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let groups = body["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0]["total"], 2);
        assert_eq!(groups[0]["issues"][0]["id"], member_ids[offset].to_string());
        assert_eq!(groups[1]["id"], "assignee:unassigned");
        assert_eq!(groups[1]["assignee_type"], Value::Null);
        assert_eq!(groups[1]["assignee_id"], Value::Null);
        assert_eq!(groups[1]["total"], 2);
        assert_eq!(
            groups[1]["issues"][0]["id"],
            unassigned_ids[offset].to_string()
        );
    }
    let (status, none) = grouped_call(
        &app,
        ws,
        "group_assignee_type=none&sort=property:00000000-0000-0000-0000-000000000001",
        Some(&auth),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{none}");
    assert_eq!(none["groups"].as_array().unwrap().len(), 1);
    assert_eq!(none["groups"][0]["total"], 2);
    assert_eq!(none["groups"][0]["id"], "assignee:unassigned");
    assert_eq!(
        grouped_call(&app, ws, "offset=2&limit=1", Some(&auth))
            .await
            .1["groups"],
        json!([])
    );
    let (_, ignored) = grouped_call(
        &app,
        ws,
        "open_only=true&ids=invalid&q=nomatch&scheduled=true",
        Some(&auth),
    )
    .await;
    assert_eq!(ignored["groups"].as_array().unwrap().len(), 2);
    let (_, labeled) = grouped_call(
        &app,
        ws,
        &format!("label_ids={label}&project_ids={project}"),
        Some(&auth),
    )
    .await;
    assert_eq!(labeled["groups"].as_array().unwrap().len(), 1);
    assert_eq!(
        labeled["groups"][0]["issues"][0]["labels"][0]["id"],
        label.to_string()
    );
    let (_, property_group) = grouped_call(
        &app,
        ws,
        &format!("properties=%7B%22{property}%22%3A%5B%22__none__%22%5D%7D"),
        Some(&auth),
    )
    .await;
    assert_eq!(property_group["groups"].as_array().unwrap().len(), 1);
    assert_eq!(
        property_group["groups"][0]["issues"][0]["id"],
        issues[3].to_string()
    );
    assert_eq!(
        property_group["groups"][0]["issues"][0]["duplicate_of"]["id"],
        issues[0].to_string()
    );
    let (_, window) = grouped_call(
        &app,
        ws,
        "date_field=created_at&date_start=2026-10-01T00:00:00Z&date_end=2026-10-02T00:00:00Z",
        Some(&auth),
    )
    .await;
    assert_eq!(window["groups"].as_array().unwrap().len(), 2);
    assert_eq!(window["groups"][0]["total"], 2);
    assert_eq!(window["groups"][1]["total"], 2);
    let (_, empty_window) = grouped_call(
        &app,
        ws,
        "date_field=created_at&date_start=2026-09-30T00:00:00Z&date_end=2026-10-01T00:00:00Z",
        Some(&auth),
    )
    .await;
    assert_eq!(empty_window["groups"], json!([]));
}
