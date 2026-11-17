//! Reading the inventory: filtered, and paged by keyset on (full_name, id) so a page never
//! shifts under a reader while a reconcile writes.

use base64::Engine as _;
use chrono::{DateTime, Utc};
use repoplane_proto::api::{Repository, RepositoryStatus, Selector, Unselected};
use repoplane_proto::forge::ForgeKind;
use repoplane_proto::id::{ConnectionId, RepositoryId};
use uuid::Uuid;

use crate::{Result, Store, StoreError};

#[derive(Debug, Default, Clone)]
pub struct RepoFilter {
    pub connection: Option<ConnectionId>,
    pub select: Selector,
    /// Include repositories absent from the latest complete listing.
    pub include_inaccessible: bool,
    pub cursor: Option<String>,
    pub limit: i64,
}

pub struct RepoPage {
    pub items: Vec<Repository>,
    pub next_cursor: Option<String>,
    /// On the first page only: what the selector could not decide, per connection.
    pub unselected: Vec<Unselected>,
}

/// A selector as a SQL condition on `repositories` (ADR 0015), its parameters numbered from
/// `first`: topics, name pattern, archived. True selects; null is unknown (a forge without
/// topics); false rules out, whatever else is unknown.
pub(crate) fn selector_condition(first: usize) -> String {
    let (t, n, a) = (first, first + 1, first + 2);
    format!(
        "(cardinality(${t}::text[]) = 0 or topics && ${t}::text[])
         and (${n}::text is null or name ilike ${n})
         and (${a}::boolean is null or archived = ${a})"
    )
}

/// The selector's values in the order [`selector_condition`] numbers them.
pub(crate) struct SelectorArgs {
    pub topics: Vec<String>,
    pub name_like: Option<String>,
    pub archived: Option<bool>,
}

impl SelectorArgs {
    pub(crate) fn new(s: &Selector) -> Result<SelectorArgs> {
        s.validate().map_err(StoreError::Invalid)?;
        Ok(SelectorArgs {
            topics: s.topic.clone(),
            name_like: s.name.as_deref().map(like_pattern),
            archived: s.archived,
        })
    }
}

/// A name pattern as a `like` pattern: `*` is any run of characters, anything else is itself.
fn like_pattern(glob: &str) -> String {
    let mut out = String::with_capacity(glob.len());
    for c in glob.chars() {
        match c {
            '*' => out.push('%'),
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out
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
        let sel = SelectorArgs::new(&f.select)?;
        let rows: Vec<RepoRow> = sqlx::query_as(&format!(
            "select id, connection_id, external_id, full_name, name, default_branch, archived,
                    topics, head_sha, head_observed_at, empty, web_url, status, first_seen_at, last_seen_at
             from repositories
             where organization_id = $1
               and ($2::uuid is null or connection_id = $2)
               and ({})
               and ($6 or status = 'active')
               and ($7::text is null or (full_name, id) > ($7, $8::uuid))
             order by full_name, id
             limit $9",
            selector_condition(3)
        ))
        .bind(self.org.uuid())
        .bind(f.connection.map(|c| c.uuid()))
        .bind(&sel.topics)
        .bind(&sel.name_like)
        .bind(sel.archived)
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
        let unselected = if f.cursor.is_none() && !f.select.topic.is_empty() {
            self.unselected(f, &sel).await?
        } else {
            Vec::new()
        };
        Ok(RepoPage {
            items,
            next_cursor,
            unselected,
        })
    }

    /// The repositories the selector is null for, per connection: the ones a forge without
    /// topics cannot say anything about.
    async fn unselected(&self, f: &RepoFilter, sel: &SelectorArgs) -> Result<Vec<Unselected>> {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(&format!(
            "select c.name, c.forge, u.n
             from (select connection_id, count(*) as n
                   from repositories
                   where organization_id = $1
                     and ($2::uuid is null or connection_id = $2)
                     and ($6 or status = 'active')
                     and ({}) is null
                   group by connection_id) u
             join connections c on c.id = u.connection_id
             order by c.name",
            selector_condition(3)
        ))
        .bind(self.org.uuid())
        .bind(f.connection.map(|c| c.uuid()))
        .bind(&sel.topics)
        .bind(&sel.name_like)
        .bind(sel.archived)
        .bind(f.include_inaccessible)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(connection, forge, count)| {
                let forge: ForgeKind = forge.parse().map_err(StoreError::Invalid)?;
                Ok(Unselected {
                    connection,
                    count,
                    reason: format!("{forge} has no repository topics"),
                })
            })
            .collect()
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
    fn a_name_pattern_becomes_a_like_pattern() {
        assert_eq!(like_pattern("*-starter"), "%-starter");
        assert_eq!(like_pattern("s4a-roma-*-starter"), "s4a-roma-%-starter");
        // What `like` would read as a wildcard is the repository's own character.
        assert_eq!(like_pattern("my_repo%"), "my\\_repo\\%");
        assert_eq!(like_pattern("a\\b"), "a\\\\b");
    }

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
