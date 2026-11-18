//! Authoritative actor context. Client identity headers never grant authority.
use super::{ApiError, error, temporarily_disabled};
use axum::http::{HeaderMap, StatusCode, Uri};
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;
use uuid::Uuid;

#[derive(Clone)]
pub(super) enum Actor {
    Human,
    Task(TaskIdentity),
    Cloud,
    Daemon,
}

#[derive(Clone)]
pub(super) struct TaskIdentity {
    pub user: Uuid,
    pub agent: Uuid,
    pub task: Uuid,
    pub workspace: Uuid,
}

impl Actor {
    pub fn require_human(&self) -> Result<(), ApiError> {
        match self {
            Self::Human => Ok(()),
            Self::Task(_) | Self::Cloud | Self::Daemon => Err(error(
                StatusCode::FORBIDDEN,
                "this endpoint is only available to human actors",
            )),
        }
    }

    pub fn workspace(&self) -> Option<Uuid> {
        match self {
            Self::Task(task) => Some(task.workspace),
            Self::Human | Self::Cloud | Self::Daemon => None,
        }
    }

    // Go's URL workspace guard checks the literal binding before UUID parsing.
    pub fn require_workspace_url(&self, raw: &str) -> Result<(), ApiError> {
        if let Some(bound) = self.workspace()
            && raw != bound.to_string()
        {
            return Err(workspace_mismatch());
        }
        Ok(())
    }
}

fn workspace_mismatch() -> ApiError {
    error(
        StatusCode::FORBIDDEN,
        "task token is bound to a different workspace",
    )
}

pub(super) async fn authenticate_task(
    db: &impl GenericClient,
    token: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<TaskIdentity, ApiError> {
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    // No cache: revocation, re-pairing and generation changes fence the next
    // request, even when the token's owner is a non-shadow runtime owner.
    let row = db
        .query_opt(include_str!("task_token.sql"), &[&hash])
        .await
        .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
    let identity = TaskIdentity {
        user: row.get("user_id"),
        agent: row.get("agent_id"),
        task: row.get("task_id"),
        workspace: row.get("workspace_id"),
    };
    if temporarily_disabled(identity.user, "") {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    check_workspace_target(db, headers, uri, identity.workspace).await?;
    Ok(identity)
}

// net/url.ParseQuery refuses malformed escapes and unescaped semicolons in ANY
// pair, not only workspace selectors. form_urlencoded alone silently repairs
// those bytes, so validate the complete query before decoding any selectors.
type QueryPairs = Vec<(Vec<u8>, Vec<u8>)>;

fn strict_params(raw: &str) -> Result<QueryPairs, ApiError> {
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b';' => return Err(error(StatusCode::BAD_REQUEST, "malformed query string")),
            b'%' => {
                if i + 2 >= bytes.len()
                    || !bytes[i + 1].is_ascii_hexdigit()
                    || !bytes[i + 2].is_ascii_hexdigit()
                {
                    return Err(error(StatusCode::BAD_REQUEST, "malformed query string"));
                }
                i += 2;
            }
            _ => {}
        }
        i += 1;
    }
    fn decode(raw: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::with_capacity(raw.len());
        let mut i = 0;
        while i < raw.len() {
            match raw[i] {
                b'%' => {
                    decoded.push(
                        (raw[i + 1].to_ascii_lowercase() as char)
                            .to_digit(16)
                            .unwrap() as u8
                            * 16
                            + (raw[i + 2].to_ascii_lowercase() as char)
                                .to_digit(16)
                                .unwrap() as u8,
                    );
                    i += 3;
                }
                b'+' => {
                    decoded.push(b' ');
                    i += 1;
                }
                c => {
                    decoded.push(c);
                    i += 1;
                }
            }
        }
        decoded
    }
    Ok(bytes
        .split(|c| *c == b'&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let eq = pair.iter().position(|c| *c == b'=').unwrap_or(pair.len());
            let value = if eq == pair.len() {
                &[][..]
            } else {
                &pair[eq + 1..]
            };
            (decode(&pair[..eq]), decode(value))
        })
        .collect())
}

async fn check_workspace_target(
    db: &impl GenericClient,
    headers: &HeaderMap,
    uri: &Uri,
    bound: Uuid,
) -> Result<(), ApiError> {
    let params = strict_params(uri.query().unwrap_or_default())?;
    // Every repeated header and query value must agree, including selectors
    // lower in the human resolver's priority order.
    for raw in headers
        .get_all("x-workspace-id")
        .iter()
        .map(|h| h.as_bytes())
        .chain(
            params
                .iter()
                .filter(|(k, _)| k == b"workspace_id")
                .map(|(_, v)| v.as_slice()),
        )
        .filter(|v| !v.is_empty())
    {
        let raw = std::str::from_utf8(raw)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
        let id = Uuid::parse_str(raw)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
        if id != bound {
            return Err(workspace_mismatch());
        }
    }
    for slug in headers
        .get_all("x-workspace-slug")
        .iter()
        .map(|h| h.as_bytes())
        .chain(
            params
                .iter()
                .filter(|(k, _)| k == b"workspace_slug")
                .map(|(_, v)| v.as_slice()),
        )
        .filter(|v| !v.is_empty())
    {
        // Go retains arbitrary decoded bytes; PostgreSQL rejects invalid UTF-8
        // or NUL text. Do not repair it into a different, potentially valid slug.
        let slug = std::str::from_utf8(slug).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve workspace",
            )
        })?;
        let row = db
            .query_opt("SELECT id FROM workspace WHERE slug=$1", &[&slug])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to resolve workspace",
                )
            })?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
        if row.get::<_, Uuid>(0) != bound {
            return Err(workspace_mismatch());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_machine_family_is_denied_by_human_guard() {
        assert!(Actor::Human.require_human().is_ok());
        for actor in [
            Actor::Daemon,
            Actor::Cloud,
            Actor::Task(TaskIdentity {
                user: Uuid::nil(),
                agent: Uuid::nil(),
                task: Uuid::nil(),
                workspace: Uuid::nil(),
            }),
        ] {
            assert_eq!(actor.require_human().unwrap_err().0, StatusCode::FORBIDDEN);
        }
    }

    #[test]
    fn query_parser_rejects_malformed_pairs_without_dropping_duplicates() {
        for raw in ["workspace_id=%zz", "other=%", "x=a;b", "workspace_slug=%1"] {
            assert_eq!(strict_params(raw).unwrap_err().0, StatusCode::BAD_REQUEST);
        }
        assert_eq!(
            strict_params("workspace_id=&workspace_id=A&x=a%3Bb&workspace_slug=a+b").unwrap(),
            vec![
                (b"workspace_id".to_vec(), b"".to_vec()),
                (b"workspace_id".to_vec(), b"A".to_vec()),
                (b"x".to_vec(), b"a;b".to_vec()),
                (b"workspace_slug".to_vec(), b"a b".to_vec()),
            ]
        );
    }
}
