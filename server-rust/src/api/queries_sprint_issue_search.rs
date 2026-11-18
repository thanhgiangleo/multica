//! Workspace-scoped interactive issue search with Go's candidate and ranking semantics.
use super::{
    ApiError, ApiState, GoJson,
    catalog::{WorkspaceId, parse_params},
    error, queries_sprint_search_shapes as shape,
};
use axum::{
    Router,
    extract::{Extension, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use serde_json::{Value, json};
use tokio_postgres::{error::SqlState, types::ToSql};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/search", get(search))
}

struct SearchSql {
    text: String,
    args: Vec<Box<dyn ToSql + Sync + Send>>,
}

fn bind<T: ToSql + Sync + Send + 'static>(
    args: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    value: T,
) -> String {
    args.push(Box::new(value));
    format!("${}", args.len())
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(super) fn query_number(q: &str) -> Option<i32> {
    let trimmed = q.trim();
    let number = if let Some((prefix, number)) = trimmed.split_once('-') {
        if !prefix.starts_with(|c: char| c.is_ascii_alphabetic())
            || !prefix.chars().all(|c| c.is_ascii_alphanumeric())
            || number.is_empty()
            || !number.bytes().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        number
    } else {
        trimmed
    };
    number.parse::<i32>().ok().filter(|v| *v > 0)
}

fn and_terms(prefix: &str, count: usize) -> String {
    (0..count)
        .map(|index| format!("{prefix}{index}"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn search_sql(
    phrase: &str,
    ws: Uuid,
    include_closed: bool,
    terminal: Vec<String>,
    limit: i64,
    offset: i64,
) -> SearchSql {
    let terms: Vec<_> = phrase.split_whitespace().map(str::to_lowercase).collect();
    let escaped = escape_like(&phrase.to_lowercase());
    let mut args: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let exact = bind(&mut args, escaped.clone());
    let contains = bind(&mut args, format!("%{escaped}%"));
    let starts = bind(&mut args, format!("{escaped}%"));
    let workspace = bind(&mut args, ws);
    let term_params: Vec<String> = if terms.len() > 1 {
        terms
            .iter()
            .map(|term| bind(&mut args, format!("%{}%", escape_like(term))))
            .collect()
    } else {
        Vec::new()
    };
    let number = query_number(phrase).map(|n| bind(&mut args, n));
    let terminal_param = (!include_closed).then(|| bind(&mut args, terminal));
    let limit_param = bind(&mut args, limit);
    let offset_param = bind(&mut args, offset);
    let title = "lowered_title.lowered";
    let description = "lowered_description.lowered";
    let comment = "lowered_comment.lowered";
    let title_matches = if term_params.is_empty() {
        format!("{title} LIKE {contains}")
    } else {
        format!(
            "({title} LIKE {contains} OR ({}))",
            term_params
                .iter()
                .map(|p| format!("{title} LIKE {p}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    };
    let mut issue_flags = vec![
        "i.id AS issue_id".to_owned(),
        "i.status".to_owned(),
        "i.updated_at".to_owned(),
        format!("{title} = {exact} AS title_exact"),
        format!("{title} LIKE {starts} AS title_starts_with"),
        format!("{title} LIKE {contains} AS title_phrase"),
        format!("COALESCE({description} LIKE {contains},FALSE) AS description_phrase"),
    ];
    if let Some(number) = &number {
        issue_flags.push(format!("i.number = {number} AS number_exact"));
    }
    for (index, param) in term_params.iter().enumerate() {
        issue_flags.push(format!("{title} LIKE {param} AS title_term_{index}"));
        issue_flags.push(format!(
            "COALESCE({description} LIKE {param},FALSE) AS description_term_{index}"
        ));
    }
    let status_filter = terminal_param
        .map(|p| format!(" AND NOT (i.status = ANY({p}::text[]))"))
        .unwrap_or_default();
    let issue_matches = format!(
        "issue_matches AS (SELECT {} FROM issue i \
         CROSS JOIN LATERAL (SELECT lower(i.title) AS lowered OFFSET 0) lowered_title \
         LEFT JOIN LATERAL (SELECT lower(COALESCE(i.description,'')) AS lowered WHERE NOT {title_matches} OFFSET 0) lowered_description ON TRUE \
         WHERE i.workspace_id={workspace}{status_filter})",
        issue_flags.join(", ")
    );

    // Aggregate comment flags separately: terms may occur in different comments.
    // Ordered aggregate retains only one matching comment ID per issue, not text.
    let mut comment_flags = vec![
        "c.issue_id".to_owned(),
        format!("BOOL_OR({comment} LIKE {contains}) AS comment_phrase"),
    ];
    let mut comment_candidates = vec!["ac.comment_phrase".to_owned()];
    let mut comment_snippet = format!("{comment} LIKE {contains}");
    for (index, param) in term_params.iter().enumerate() {
        comment_flags.push(format!(
            "BOOL_OR({comment} LIKE {param}) AS comment_term_{index}"
        ));
        comment_candidates.push(format!("ac.comment_term_{index}"));
    }
    if !term_params.is_empty() {
        let all = term_params
            .iter()
            .map(|p| format!("{comment} LIKE {p}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        comment_flags.push(format!("BOOL_OR({all}) AS comment_all_terms"));
        comment_snippet.push_str(&format!(" OR ({all})"));
    }
    comment_flags.push(format!(
        "(ARRAY_AGG(c.id ORDER BY c.created_at DESC,c.id DESC) FILTER (WHERE {comment_snippet}))[1] AS snippet_comment_id"
    ));
    let comment_matches = format!(
        "comment_matches AS MATERIALIZED (SELECT * FROM (SELECT {} FROM comment c \
         CROSS JOIN LATERAL (SELECT lower(c.content) AS lowered OFFSET 0) lowered_comment \
         WHERE c.workspace_id={workspace} GROUP BY c.issue_id) ac WHERE {})",
        comment_flags.join(", "),
        comment_candidates.join(" OR ")
    );

    let mut eligibility = vec![
        "im.title_phrase".to_owned(),
        "im.description_phrase".to_owned(),
        "COALESCE(cm.comment_phrase,FALSE)".to_owned(),
    ];
    let mut relevance = Vec::new();
    if number.is_some() {
        eligibility.push("im.number_exact".to_owned());
        relevance.push("WHEN im.number_exact THEN 0".to_owned());
    }
    relevance.extend([
        "WHEN im.title_exact THEN 1".to_owned(),
        "WHEN im.title_starts_with THEN 2".to_owned(),
        "WHEN im.title_phrase THEN 3".to_owned(),
    ]);
    let mut source = vec!["WHEN im.title_phrase THEN 'title'".to_owned()];
    if !term_params.is_empty() {
        let all_spread = (0..term_params.len())
            .map(|n| format!("(im.title_term_{n} OR im.description_term_{n} OR COALESCE(cm.comment_term_{n},FALSE))"))
            .collect::<Vec<_>>()
            .join(" AND ");
        eligibility.push(format!("({all_spread})"));
        let all_title = and_terms("im.title_term_", term_params.len());
        relevance.push(format!("WHEN ({all_title}) THEN 4"));
        source.push(format!("WHEN ({all_title}) THEN 'title'"));
    }
    relevance.push("WHEN im.description_phrase THEN 5".to_owned());
    source.push("WHEN im.description_phrase THEN 'description'".to_owned());
    if !term_params.is_empty() {
        let all_desc = and_terms("im.description_term_", term_params.len());
        relevance.push(format!("WHEN ({all_desc}) THEN 6"));
        source.push(format!("WHEN ({all_desc}) THEN 'description'"));
    }
    relevance.push("WHEN COALESCE(cm.comment_phrase,FALSE) THEN 7".to_owned());
    if !term_params.is_empty() {
        relevance.push("WHEN COALESCE(cm.comment_all_terms,FALSE) THEN 8".to_owned());
    }
    let direct = if number.is_some() {
        "im.title_exact OR im.number_exact"
    } else {
        "im.title_exact"
    };
    let status_rank = "CASE im.status WHEN 'in_progress' THEN 0 WHEN 'in_review' THEN 1 WHEN 'todo' THEN 2 WHEN 'blocked' THEN 3 WHEN 'backlog' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END";
    let ranked = format!(
        "ranked_candidates AS (SELECT im.issue_id,im.updated_at,cm.snippet_comment_id, \
         CASE WHEN im.status='cancelled' AND NOT ({direct}) THEN 1 ELSE 0 END AS cancelled_rank, \
         CASE {} ELSE 9 END AS relevance_rank, {status_rank} AS status_rank, \
         CASE {} ELSE 'comment' END AS match_source \
         FROM issue_matches im LEFT JOIN comment_matches cm ON cm.issue_id=im.issue_id WHERE ({}))",
        relevance.join(" "),
        source.join(" "),
        eligibility.join(" OR ")
    );
    let page = format!(
        "page_candidates AS MATERIALIZED (SELECT * FROM ranked_candidates \
         ORDER BY cancelled_rank,relevance_rank,status_rank,updated_at DESC,issue_id ASC \
         LIMIT {limit_param}::int8 OFFSET {offset_param}::int8)"
    );
    let projection = shape::ISSUE_SELECT
        .strip_prefix("SELECT ")
        .unwrap()
        .split_once(" FROM issue i")
        .unwrap()
        .0;
    SearchSql {
        text: format!(
            "WITH {issue_matches}, {comment_matches}, {ranked}, {page} \
             SELECT {projection}, pc.match_source, COALESCE(c.content,'') AS matched_comment_content \
             FROM page_candidates pc JOIN issue i ON i.id=pc.issue_id AND i.workspace_id={workspace} \
             JOIN workspace w ON w.id=i.workspace_id \
             LEFT JOIN issue_status st ON st.workspace_id=i.workspace_id AND st.key=i.status \
             LEFT JOIN issue original ON i.status='cancelled' AND original.id=i.duplicate_of_issue_id AND original.workspace_id=i.workspace_id \
             LEFT JOIN comment c ON c.id=pc.snippet_comment_id AND c.workspace_id={workspace} \
             ORDER BY pc.cancelled_rank,pc.relevance_rank,pc.status_rank,pc.updated_at DESC,pc.issue_id ASC"
        ),
        args,
    }
}

fn snippet(content: &str, query: &str) -> String {
    let chars: Vec<char> = content.chars().collect();
    // Keep one folded code point per source character so snippet offsets stay valid.
    let fold = |c: char| c.to_lowercase().next().unwrap_or(c);
    let lowered: Vec<char> = chars.iter().copied().map(fold).collect();
    let phrase: Vec<char> = query.chars().map(fold).collect();
    fn position(haystack: &[char], needle: &[char]) -> Option<usize> {
        if needle.is_empty() {
            None
        } else {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        }
    }
    let mut hit = position(&lowered, &phrase).map(|pos| (pos, phrase.len()));
    if hit.is_none() {
        let terms: Vec<_> = query.split_whitespace().collect();
        if terms.len() > 1 {
            for term in terms {
                let lower: Vec<_> = term.chars().map(fold).collect();
                if let Some(pos) = position(&lowered, &lower)
                    && hit.is_none_or(|(prev, _)| pos < prev)
                {
                    hit = Some((pos, lower.len()));
                }
            }
        }
    }
    let Some((pos, len)) = hit else {
        return if chars.len() > 120 {
            format!("{}...", chars[..120].iter().collect::<String>())
        } else {
            content.to_owned()
        };
    };
    let start = pos.saturating_sub(40);
    let end = (pos + len + 80).min(chars.len());
    format!(
        "{}{}{}",
        if start > 0 { "..." } else { "" },
        chars[start..end].iter().collect::<String>(),
        if end < chars.len() { "..." } else { "" }
    )
}

fn description_contains(description: &str, phrase: &str) -> bool {
    let lower = description.to_lowercase();
    lower.contains(&phrase.to_lowercase()) || {
        let terms: Vec<_> = phrase.split_whitespace().collect();
        terms.len() > 1
            && terms
                .iter()
                .all(|term| lower.contains(&term.to_lowercase()))
    }
}

async fn search(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let phrase = params
        .get("q")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "q parameter is required"))?;
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(20)
        .min(50);
    let offset = params
        .get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(0);
    let include_closed = params.get("include_closed").is_some_and(|s| s == "true");
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues"))?;
    let terminal = if include_closed {
        vec![]
    } else {
        let rows=db.query("SELECT key FROM issue_status WHERE workspace_id=$1 AND category IN ('done','closed')",&[&ws]).await
            .map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to resolve status categories"))?;
        let mut keys: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        for key in ["done", "cancelled"] {
            if !keys.iter().any(|existing| existing == key) {
                keys.push(key.to_owned());
            }
        }
        keys
    };
    let sql = search_sql(phrase, ws, include_closed, terminal, limit, offset);
    let tx = db
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues"))?;
    let work_mem = std::env::var("DATABASE_SEARCH_WORK_MEM_MB")
        .ok()
        .and_then(|s| s.trim().parse::<u8>().ok())
        .filter(|n| *n <= 64)
        .unwrap_or(64);
    tx.batch_execute("SET LOCAL statement_timeout=8000")
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues"))?;
    if work_mem > 0 {
        tx.batch_execute(&format!("SET LOCAL work_mem='{work_mem}MB'"))
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues"))?;
    }
    let args: Vec<&(dyn ToSql + Sync)> = sql
        .args
        .iter()
        .map(|arg| arg.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = tx.query(sql.text.as_str(), &args).await.map_err(|err| {
        if err.code() == Some(&SqlState::QUERY_CANCELED) {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "search timed out; please refine your query or try again",
            )
        } else {
            error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues")
        }
    })?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search issues"))?;
    let issues: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut value = shape::issue(row);
            value.as_object_mut().unwrap().remove("search_updated_at");
            let source: &str = row.get("match_source");
            value["match_source"] = json!(source);
            let comment: String = row.get("matched_comment_content");
            if !comment.is_empty() {
                let text = snippet(&comment, phrase);
                value["matched_comment_snippet"] = json!(text);
                if source == "comment" {
                    value["matched_snippet"] = json!(text);
                }
            }
            let description: Option<String> = row.get("description");
            if let Some(description) = description
                && !description.is_empty()
                && (source == "description" || description_contains(&description, phrase))
            {
                value["matched_description_snippet"] = json!(snippet(&description, phrase));
            }
            value
        })
        .collect();
    Ok(GoJson(json!({"issues":issues})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifier_and_utf8_snippet_match_frozen_go() {
        assert_eq!(query_number("V2-123"), Some(123));
        assert_eq!(query_number("2147483648"), None);
        assert_eq!(query_number("V-0"), None);
        assert_eq!(snippet("a".repeat(45).as_str(), "z"), "a".repeat(45));
        assert_eq!(snippet("🐱 abc 🐱", "abc"), "🐱 abc 🐱");
        let expanded_lowercase = format!("{} match", "İ".repeat(200));
        assert_eq!(
            snippet(&expanded_lowercase, "match"),
            format!("...{} match", "İ".repeat(39))
        );
        assert_eq!(snippet("ΟΣ", "οσ"), "ΟΣ");
    }
    #[tokio::test]
    #[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
    async fn postgres_search_candidate_index_eligibility() {
        use std::str::FromStr;
        use tokio_postgres::{Config, NoTls};

        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
        assert!(config.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(name) if name == "127.0.0.1")
        ));
        assert!(config.get_ports().iter().all(|port| *port != 5432));
        let (mut db, connection) = config.connect(NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        let tx = db.transaction().await.unwrap();
        tx.batch_execute("SET LOCAL enable_seqscan=off; SET LOCAL statement_timeout=8000")
            .await
            .unwrap();
        let sql = search_sql(
            "alpha beta",
            Uuid::new_v4(),
            false,
            vec!["done".into()],
            20,
            0,
        );
        let args: Vec<&(dyn ToSql + Sync)> = sql
            .args
            .iter()
            .map(|arg| arg.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let plan: Value = tx
            .query_one(&format!("EXPLAIN (FORMAT JSON) {}", sql.text), &args)
            .await
            .unwrap()
            .get(0);
        let text = plan.to_string();
        // Eligibility only: disabling seqscan is not a performance benchmark.
        assert!(text.contains("idx_comment_workspace"), "{text}");
        assert!(
            text.contains("idx_issue_workspace") || text.contains("idx_issue_status"),
            "{text}"
        );
        assert!(text.contains("workspace_id"), "{text}");
        println!("issue search workspace index eligibility: {plan}");
        tx.rollback().await.unwrap();
        drop(db);
        connection.await.unwrap().unwrap();
    }
    #[test]
    fn query_is_scoped_and_uses_bind_parameters() {
        let ws = Uuid::new_v4();
        let sql = search_sql("alpha beta", ws, false, vec!["done".into()], 20, 0);
        assert!(sql.text.contains("i.workspace_id=$4"));
        assert!(sql.text.contains("c.workspace_id=$4"));
        assert!(sql.text.contains(
            "im.title_term_0 OR im.description_term_0 OR COALESCE(cm.comment_term_0,FALSE)"
        ));
        assert!(!sql.text.contains("alpha beta"));
    }
}
