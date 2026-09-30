//! Reading the inventory: filtered, and paged by keyset on (full_name, id) so a page never
//! shifts under a reader while a reconcile writes.

use base64::Engine as _;
use chrono::{DateTime, Utc};
use repoplane_proto::api::{Repository, RepositoryStatus};
use repoplane_proto::id::{ConnectionId, RepositoryId};
use uuid::Uuid;

use crate::{Result, Store, StoreError};

#[derive(Debug, Default, Clone)]
pub struct RepoFilter {
    pub connection: Option<ConnectionId>,
    pub topic: Option<String>,
    pub archived: Option<bool>,
    /// Include repositories absent from the latest complete listing.
    pub include_inaccessible: bool,
    pub cursor: Option<String>,
    pub limit: i64,
}

pub struct RepoPage {
    pub items: Vec<Repository>,
    pub next_cursor: Option<String>,
}

#[derive(sqlx::FromRow)]
struct RepoRow {
    id: Uuid,
    connection_id: Uuid,
    external_id: String,
    full_name: String,
    name: String,
    default_branch: Option<String>,
    archived: bool,
    topics: Option<Vec<String>>,
    head_sha: Option<String>,
    head_observed_at: Option<DateTime<Utc>>,
    empty: bool,
    web_url: Option<String>,
    status: String,
    first_seen_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
}

impl Store {
    pub async fn repositories(&self, f: &RepoFilter) -> Result<RepoPage> {
        let limit = f.limit.clamp(1, 1000);
        let (after_name, after_id) = match &f.cursor {
            Some(c) => {
                let (n, i) = decode_cursor(c)?;
                (Some(n), Some(i))
            }
            None => (None, None),
        };
        let rows: Vec<RepoRow> = sqlx::query_as(
            "select id, connection_id, external_id, full_name, name, default_branch, archived,
                    topics, head_sha, head_observed_at, empty, web_url, status, first_seen_at, last_seen_at
             from repositories
             where organization_id = $1
               and ($2::uuid is null or connection_id = $2)
               and ($3::text is null or $3 = any(topics))
               and ($4::boolean is null or archived = $4)
               and ($5 or status = 'active')
               and ($6::text is null or (full_name, id) > ($6, $7::uuid))
             order by full_name, id
             limit $8",
        )
        .bind(self.org.uuid())
        .bind(f.connection.map(|c| c.uuid()))
        .bind(&f.topic)
        .bind(f.archived)
        .bind(f.include_inaccessible)
        .bind(after_name)
        .bind(after_id)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let more = rows.len() as i64 > limit;
        let items: Vec<Repository> = rows
            .into_iter()
            .take(limit as usize)
            .map(RepoRow::into_api)
            .collect::<Result<_>>()?;
        let next_cursor = match (more, items.last()) {
            (true, Some(last)) => Some(encode_cursor(&last.full_name, last.id.uuid())),
            _ => None,
        };
        Ok(RepoPage { items, next_cursor })
    }
}

impl RepoRow {
    fn into_api(self) -> Result<Repository> {
        Ok(Repository {
            id: RepositoryId::from_uuid(self.id),
            connection_id: ConnectionId::from_uuid(self.connection_id),
            external_id: self.external_id,
            full_name: self.full_name,
            name: self.name,
            default_branch: self.default_branch,
            archived: self.archived,
            topics: self.topics,
            head: self.head_sha,
            head_observed_at: self.head_observed_at,
            empty: self.empty,
            web_url: self.web_url,
            status: match self.status.as_str() {
                "active" => RepositoryStatus::Active,
                "inaccessible" => RepositoryStatus::Inaccessible,
                other => return Err(StoreError::Invalid(format!("unknown status {other:?}"))),
            },
            first_seen_at: self.first_seen_at,
            last_seen_at: self.last_seen_at,
        })
    }
}

fn encode_cursor(full_name: &str, id: Uuid) -> String {
    let raw = serde_json::to_vec(&(full_name, id)).expect("a tuple encodes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

fn decode_cursor(c: &str) -> Result<(String, Uuid)> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(c)
        .ok()
        .and_then(|raw| serde_json::from_slice(&raw).ok())
        .ok_or_else(|| StoreError::Invalid(format!("cursor {c:?} is not one this API issued")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let id = Uuid::now_v7();
        let c = encode_cursor("acme/payments/api", id);
        assert_eq!(
            decode_cursor(&c).unwrap(),
            ("acme/payments/api".to_string(), id)
        );
        assert!(decode_cursor("not-a-cursor").is_err());
    }
}
