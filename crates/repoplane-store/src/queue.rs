//! The task queue. A worker leases the oldest ready task on its queue — queued, or leased by a
//! worker that stopped reporting — with `FOR UPDATE SKIP LOCKED`, so two workers never take the
//! same one. Every later call names the lease; once a lease has been handed on, the old holder's
//! calls fail with `LeaseLost` and it stops.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use repoplane_proto::id::{ConnectionId, ReconcileId, TaskId};
use repoplane_proto::worker::{ConnectionSpec, RepoSnapshot, Work};
use uuid::Uuid;

use crate::{Result, Store, StoreError};

/// How many times a retryable failure is retried before the run fails.
pub const MAX_ATTEMPTS: i32 = 5;

pub struct Leased {
    pub task: TaskId,
    pub lease_id: Uuid,
    pub work: Work,
}

pub struct UploadOutcome {
    pub recorded: i64,
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    task_id: Uuid,
    lease_id: Uuid,
    reconcile_id: Uuid,
    connection_id: Uuid,
    forge: String,
    org: String,
    base_url: String,
    token_env: String,
}

#[derive(sqlx::FromRow)]
struct Existing {
    external_id: String,
    full_name: String,
    name: String,
    default_branch: Option<String>,
    archived: bool,
    topics: Option<Vec<String>>,
    head_sha: Option<String>,
    empty: bool,
    web_url: Option<String>,
    status: String,
    last_seen_at: DateTime<Utc>,
}

impl Store {
    /// Takes the oldest ready task on `queue`, or returns None. The lease lasts `ttl_secs`
    /// unless the worker reports progress.
    pub async fn lease(&self, queue: &str, ttl_secs: i64) -> Result<Option<Leased>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<LeaseRow> = sqlx::query_as(
            "with next as (
                 select id from tasks
                 where organization_id = $1 and queue = $2
                   and (state = 'queued' or (state = 'leased' and lease_expires_at < now()))
                 order by created_at
                 for update skip locked
                 limit 1
             ), leased as (
                 update tasks t
                 set state = 'leased', lease_id = gen_random_uuid(),
                     lease_expires_at = now() + make_interval(secs => $3),
                     attempts = t.attempts + 1, updated_at = now()
                 from next where t.id = next.id
                 returning t.id, t.lease_id
             )
             select l.id as task_id, l.lease_id, r.id as reconcile_id, c.id as connection_id,
                    c.forge, c.org, c.base_url, c.token_env
             from leased l
             join reconciles r on r.task_id = l.id
             join connections c on c.id = r.connection_id",
        )
        .bind(self.org.uuid())
        .bind(queue)
        .bind(ttl_secs as f64)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        // A new attempt lists from scratch under a new watermark; the counters restart with it.
        sqlx::query(
            "update reconciles set status = 'running', watermark = now(),
                    started_at = coalesce(started_at, now()), discovered = 0, recorded = 0
             where id = $1",
        )
        .bind(row.reconcile_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(Leased {
            task: TaskId::from_uuid(row.task_id),
            lease_id: row.lease_id,
            work: Work::Reconcile {
                reconcile: ReconcileId::from_uuid(row.reconcile_id),
                connection: ConnectionSpec {
                    id: ConnectionId::from_uuid(row.connection_id),
                    forge: row.forge.parse().map_err(StoreError::Invalid)?,
                    org: row.org,
                    base_url: row.base_url,
                    token_env: row.token_env,
                },
            },
        }))
    }

    /// Extends a lease. Fails with `LeaseLost` when it has been handed to another worker.
    pub async fn heartbeat(&self, task: TaskId, lease_id: Uuid, ttl_secs: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.extend(&mut tx, task, lease_id, ttl_secs).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn extend(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: TaskId,
        lease_id: Uuid,
        ttl_secs: i64,
    ) -> Result<()> {
        let n = sqlx::query(
            "update tasks set lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
             where id = $1 and lease_id = $2 and state = 'leased'",
        )
        .bind(task.uuid())
        .bind(lease_id)
        .bind(ttl_secs as f64)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(StoreError::LeaseLost);
        }
        Ok(())
    }

    /// Records one page of a listing. A repository is *recorded* when it is new, changed, or
    /// back from inaccessible; every one listed is stamped seen at the run's watermark.
    pub async fn upload_page(
        &self,
        task: TaskId,
        lease_id: Uuid,
        ttl_secs: i64,
        repos: &[RepoSnapshot],
    ) -> Result<UploadOutcome> {
        let mut tx = self.pool.begin().await?;
        self.extend(&mut tx, task, lease_id, ttl_secs).await?;
        let (reconcile_id, connection_id, watermark): (Uuid, Uuid, DateTime<Utc>) = sqlx::query_as(
            "select id, connection_id, watermark from reconciles where task_id = $1",
        )
        .bind(task.uuid())
        .fetch_one(&mut *tx)
        .await?;

        for r in repos {
            validate_snapshot(r)?;
        }
        let ids: Vec<&str> = repos.iter().map(|r| r.external_id.as_str()).collect();
        let existing: Vec<Existing> = sqlx::query_as(
            "select external_id, full_name, name, default_branch, archived, topics, head_sha,
                    empty, web_url, status, last_seen_at
             from repositories where connection_id = $1 and external_id = any($2)",
        )
        .bind(connection_id)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
        let existing: HashMap<String, Existing> = existing
            .into_iter()
            .map(|e| (e.external_id.clone(), e))
            .collect();

        let mut recorded = 0i64;
        for r in repos {
            let prior = existing.get(&r.external_id);
            // A later run already wrote this row: an older one must not overwrite it.
            if prior.is_some_and(|p| p.last_seen_at > watermark) {
                continue;
            }
            if prior.is_none_or(|p| changed(p, r)) {
                recorded += 1;
            }
            sqlx::query(
                "insert into repositories
                     (id, organization_id, connection_id, external_id, full_name, name,
                      default_branch, archived, topics, head_sha, empty, web_url, status,
                      first_seen_at, last_seen_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'active', $13, $13)
                 on conflict (connection_id, external_id) do update set
                     full_name = excluded.full_name, name = excluded.name,
                     default_branch = excluded.default_branch, archived = excluded.archived,
                     topics = excluded.topics, head_sha = excluded.head_sha,
                     empty = excluded.empty, web_url = excluded.web_url,
                     status = 'active', inaccessible_since = null,
                     last_seen_at = excluded.last_seen_at
                 where repositories.last_seen_at <= excluded.last_seen_at",
            )
            .bind(repoplane_proto::id::RepositoryId::new().uuid())
            .bind(self.org.uuid())
            .bind(connection_id)
            .bind(&r.external_id)
            .bind(&r.full_name)
            .bind(&r.name)
            .bind(&r.default_branch)
            .bind(r.archived)
            .bind(r.topics.as_ref().map(|t| sorted(t)))
            .bind(&r.head)
            .bind(r.empty)
            .bind(&r.web_url)
            .bind(watermark)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("update reconciles set recorded = recorded + $2 where id = $1")
            .bind(reconcile_id)
            .bind(recorded)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(UploadOutcome { recorded })
    }

    /// Finishes a run. Only a complete listing marks the repositories it did not see as
    /// inaccessible; an incomplete one would mark whatever sat on the pages it never read.
    pub async fn complete(
        &self,
        task: TaskId,
        lease_id: Uuid,
        complete_listing: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let (reconcile_id, connection_id, watermark): (Uuid, Uuid, DateTime<Utc>) = sqlx::query_as(
            "select r.id, r.connection_id, r.watermark from reconciles r
                 join tasks t on t.id = r.task_id
                 where t.id = $1 and t.lease_id = $2 and t.state = 'leased'
                 for update of t",
        )
        .bind(task.uuid())
        .bind(lease_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::LeaseLost)?;
        if !complete_listing {
            return Err(StoreError::Invalid(
                "complete requires a complete listing; report an incomplete one as a failure"
                    .into(),
            ));
        }
        let inaccessible = sqlx::query(
            "update repositories set status = 'inaccessible', inaccessible_since = now()
             where connection_id = $1 and status = 'active' and last_seen_at < $2",
        )
        .bind(connection_id)
        .bind(watermark)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        // Distinct repositories seen by this run: a repository met on two pages of a shifting
        // listing counts once.
        let discovered: i64 = sqlx::query_scalar(
            "select count(*) from repositories where connection_id = $1 and last_seen_at = $2",
        )
        .bind(connection_id)
        .bind(watermark)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "update reconciles set status = 'succeeded', discovered = $2, inaccessible = $3,
                    finished_at = now(), error = null
             where id = $1",
        )
        .bind(reconcile_id)
        .bind(discovered)
        .bind(inaccessible)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "update tasks set state = 'done', lease_id = null, updated_at = now() where id = $1",
        )
        .bind(task.uuid())
        .execute(&mut *tx)
        .await?;
        sqlx::query("update connections set last_reconciled_at = now() where id = $1")
            .bind(connection_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Reports a failed attempt. A retryable one goes back on the queue until it has been tried
    /// `MAX_ATTEMPTS` times; anything else fails the run, and no repository is marked.
    pub async fn fail(
        &self,
        task: TaskId,
        lease_id: Uuid,
        message: &str,
        retryable: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let attempts: i32 = sqlx::query_scalar(
            "select attempts from tasks where id = $1 and lease_id = $2 and state = 'leased' for update",
        )
        .bind(task.uuid())
        .bind(lease_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::LeaseLost)?;
        let message: String = message.chars().take(4000).collect();
        if retryable && attempts < MAX_ATTEMPTS {
            sqlx::query(
                "update tasks set state = 'queued', lease_id = null, lease_expires_at = null,
                        last_error = $2, updated_at = now()
                 where id = $1",
            )
            .bind(task.uuid())
            .bind(&message)
            .execute(&mut *tx)
            .await?;
            sqlx::query("update reconciles set status = 'queued', error = $2 where task_id = $1")
                .bind(task.uuid())
                .bind(&message)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                "update tasks set state = 'failed', lease_id = null, last_error = $2, updated_at = now()
                 where id = $1",
            )
            .bind(task.uuid())
            .bind(&message)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "update reconciles set status = 'failed', error = $2, finished_at = now() where task_id = $1",
            )
            .bind(task.uuid())
            .bind(&message)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

fn sorted(t: &[String]) -> Vec<String> {
    let mut v = t.to_vec();
    v.sort();
    v.dedup();
    v
}

fn changed(p: &Existing, r: &RepoSnapshot) -> bool {
    p.status != "active"
        || p.full_name != r.full_name
        || p.name != r.name
        || p.default_branch != r.default_branch
        || p.archived != r.archived
        || p.topics != r.topics.as_ref().map(|t| sorted(t))
        || p.head_sha != r.head
        || p.empty != r.empty
        || p.web_url != r.web_url
}

fn validate_snapshot(r: &RepoSnapshot) -> Result<()> {
    if r.external_id.is_empty() || r.full_name.is_empty() || r.name.is_empty() {
        return Err(StoreError::Invalid(format!(
            "a listed repository needs an external id, a full name and a name: {r:?}"
        )));
    }
    if let Some(h) = &r.head
        && !(matches!(h.len(), 40 | 64)
            && h.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()))
    {
        return Err(StoreError::Invalid(format!(
            "{}: head {h:?} is not a lowercase hex commit id",
            r.full_name
        )));
    }
    Ok(())
}
