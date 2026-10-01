//! The task queue. A worker leases the oldest ready task on its queue — queued, or leased by a
//! worker that stopped reporting — with `FOR UPDATE SKIP LOCKED`, so two workers never take the
//! same one. Every later call names the lease; once a lease has been handed on, the old holder's
//! calls fail with `LeaseLost` and it stops.
//!
//! Two kinds of task share it: a reconcile (list the organisation, record what exists) and a
//! head refresh (read the default-branch HEAD of chosen repositories).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use repoplane_proto::id::{ConnectionId, HeadRefreshId, ReconcileId, TaskId};
use repoplane_proto::worker::{ConnectionSpec, HeadTarget, ObservedHead, RepoSnapshot, Work};
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
    kind: String,
}

#[derive(sqlx::FromRow)]
struct ConnectionRow {
    connection_id: Uuid,
    forge: String,
    org: String,
    base_url: String,
    token_env: String,
}

impl ConnectionRow {
    fn spec(self) -> Result<ConnectionSpec> {
        Ok(ConnectionSpec {
            id: ConnectionId::from_uuid(self.connection_id),
            forge: self.forge.parse().map_err(StoreError::Invalid)?,
            org: self.org,
            base_url: self.base_url,
            token_env: self.token_env,
        })
    }
}

#[derive(sqlx::FromRow)]
struct Existing {
    external_id: String,
    full_name: String,
    name: String,
    default_branch: Option<String>,
    archived: bool,
    topics: Option<Vec<String>>,
    empty: bool,
    web_url: Option<String>,
    status: String,
    last_seen_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct Target {
    id: Uuid,
    external_id: String,
    default_branch: Option<String>,
    head_sha: Option<String>,
    head_observed_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct TargetRow {
    external_id: String,
    full_name: String,
    name: String,
    default_branch: Option<String>,
    archived: bool,
    topics: Option<Vec<String>>,
    empty: bool,
    web_url: Option<String>,
    locator: String,
}

/// The run a task belongs to.
enum Run {
    Reconcile(Uuid),
    Refresh(Uuid),
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
             )
             update tasks t
             set state = 'leased', lease_id = gen_random_uuid(),
                 lease_expires_at = now() + make_interval(secs => $3),
                 attempts = t.attempts + 1, updated_at = now()
             from next where t.id = next.id
             returning t.id as task_id, t.lease_id, t.kind",
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
        let work = match row.kind.as_str() {
            "reconcile" => {
                // A new attempt lists from scratch under a new watermark; the counters restart.
                let (id,): (Uuid,) = sqlx::query_as(
                    "update reconciles set status = 'running', watermark = now(),
                            started_at = coalesce(started_at, now()), discovered = 0, recorded = 0
                     where task_id = $1 returning id",
                )
                .bind(row.task_id)
                .fetch_one(&mut *tx)
                .await?;
                let c: ConnectionRow = sqlx::query_as(
                    "select c.id as connection_id, c.forge, c.org, c.base_url, c.token_env
                     from reconciles r join connections c on c.id = r.connection_id where r.id = $1",
                )
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                Work::Reconcile {
                    reconcile: ReconcileId::from_uuid(id),
                    connection: c.spec()?,
                }
            }
            "refresh_heads" => {
                let (id,): (Uuid,) = sqlx::query_as(
                    // Counts carry over a retry: HEADs an earlier attempt recorded are counted,
                    // and `upload_heads` counts each target once per refresh.
                    "update head_refreshes set status = 'running', started_at = coalesce(started_at, now())
                     where task_id = $1 returning id",
                )
                .bind(row.task_id)
                .fetch_one(&mut *tx)
                .await?;
                let c: ConnectionRow = sqlx::query_as(
                    "select c.id as connection_id, c.forge, c.org, c.base_url, c.token_env
                     from head_refreshes h join connections c on c.id = h.connection_id
                     where h.id = $1",
                )
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
                // The targets as the inventory holds them now: a repository gone inaccessible
                // since the refresh was requested is not read.
                let targets: Vec<TargetRow> = sqlx::query_as(
                    "select r.external_id, r.full_name, r.name, r.default_branch, r.archived,
                            r.topics, r.empty, r.web_url, r.locator
                     from head_refreshes h join repositories r on r.id = any(h.targets)
                     where h.id = $1 and r.status = 'active' and r.locator <> ''
                     order by r.full_name",
                )
                .bind(id)
                .fetch_all(&mut *tx)
                .await?;
                Work::RefreshHeads {
                    refresh: HeadRefreshId::from_uuid(id),
                    connection: c.spec()?,
                    targets: targets
                        .into_iter()
                        .map(|t| HeadTarget {
                            repo: RepoSnapshot {
                                external_id: t.external_id,
                                full_name: t.full_name,
                                name: t.name,
                                default_branch: t.default_branch,
                                archived: t.archived,
                                topics: t.topics,
                                empty: t.empty,
                                web_url: t.web_url,
                                locator: t.locator,
                            },
                        })
                        .collect(),
                }
            }
            other => return Err(StoreError::Invalid(format!("unknown task kind {other:?}"))),
        };
        tx.commit().await?;
        Ok(Some(Leased {
            task: TaskId::from_uuid(row.task_id),
            lease_id: row.lease_id,
            work,
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
    ///
    /// A listing says nothing about HEADs, so they are left alone — except when the default
    /// branch changed, where the recorded HEAD described another branch and is cleared.
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
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::Invalid("pages belong to a reconcile task".into()))?;

        for r in repos {
            validate_snapshot(r)?;
        }
        let ids: Vec<&str> = repos.iter().map(|r| r.external_id.as_str()).collect();
        let existing: Vec<Existing> = sqlx::query_as(
            "select external_id, full_name, name, default_branch, archived, topics,
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
                      default_branch, archived, topics, empty, web_url, locator, status,
                      first_seen_at, last_seen_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'active', $13, $13)
                 on conflict (connection_id, external_id) do update set
                     full_name = excluded.full_name, name = excluded.name,
                     head_sha = case when repositories.default_branch is distinct from excluded.default_branch
                                     then null else repositories.head_sha end,
                     head_observed_at = case when repositories.default_branch is distinct from excluded.default_branch
                                             then null else repositories.head_observed_at end,
                     default_branch = excluded.default_branch, archived = excluded.archived,
                     topics = excluded.topics, empty = excluded.empty,
                     web_url = excluded.web_url, locator = excluded.locator,
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
            .bind(r.empty)
            .bind(&r.web_url)
            .bind(&r.locator)
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

    /// Records HEADs a refresh read, and returns how many changed.
    ///
    /// Only the refresh's own targets are written, and only while still active. A HEAD read
    /// from a branch that is no longer the default (a reconcile recorded the change while the
    /// refresh ran) describes the old branch and is dropped. Each target counts once per
    /// refresh, retries included: *observed* when first recorded, *changed* when that differs
    /// from what was recorded before the refresh.
    pub async fn upload_heads(
        &self,
        task: TaskId,
        lease_id: Uuid,
        ttl_secs: i64,
        heads: &[ObservedHead],
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        self.extend(&mut tx, task, lease_id, ttl_secs).await?;
        let (refresh_id, started_at): (Uuid, DateTime<Utc>) =
            sqlx::query_as("select id, started_at from head_refreshes where task_id = $1")
                .bind(task.uuid())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| StoreError::Invalid("heads belong to a head refresh task".into()))?;
        for h in heads {
            if let Some(sha) = &h.head
                && !is_commit_id(sha)
            {
                return Err(StoreError::Invalid(format!(
                    "{}: head {sha:?} is not a lowercase hex commit id",
                    h.external_id
                )));
            }
        }
        let ids: Vec<&str> = heads.iter().map(|h| h.external_id.as_str()).collect();
        let targets: HashMap<String, Target> = sqlx::query_as::<_, Target>(
            "select r.id, r.external_id, r.default_branch, r.head_sha, r.head_observed_at
             from head_refreshes h join repositories r on r.id = any(h.targets)
             where h.id = $1 and r.status = 'active' and r.external_id = any($2)
             for update of r",
        )
        .bind(refresh_id)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|t| (t.external_id.clone(), t))
        .collect();
        let mut changed = 0i64;
        let mut observed = 0i64;
        for h in heads {
            let Some(t) = targets.get(&h.external_id) else {
                continue; // not a target of this refresh, or gone inaccessible since
            };
            if t.default_branch != h.branch {
                continue; // read from a branch that is no longer the default
            }
            let first_this_refresh = t.head_observed_at.is_none_or(|at| at < started_at);
            if first_this_refresh {
                observed += 1;
                if t.head_sha != h.head {
                    changed += 1;
                }
            }
            sqlx::query(
                "update repositories set head_sha = $2, head_observed_at = now() where id = $1",
            )
            .bind(t.id)
            .bind(&h.head)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "update head_refreshes set observed = observed + $2, changed = changed + $3 where id = $1",
        )
        .bind(refresh_id)
        .bind(observed)
        .bind(changed)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(changed)
    }

    /// The repositories a reconcile has not seen: active in the inventory, and not stamped with
    /// the run's watermark. The worker confirms each with a direct lookup before completing.
    pub async fn unseen(
        &self,
        task: TaskId,
        lease_id: Uuid,
        ttl_secs: i64,
    ) -> Result<Vec<(String, String)>> {
        let mut tx = self.pool.begin().await?;
        self.extend(&mut tx, task, lease_id, ttl_secs).await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "select r.external_id, r.full_name from repositories r
             join reconciles rc on rc.connection_id = r.connection_id
             where rc.task_id = $1 and r.status = 'active' and r.last_seen_at < rc.watermark
             order by r.full_name",
        )
        .bind(task.uuid())
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// The run a leased task belongs to, locking the task for the rest of the transaction.
    async fn leased_run(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: TaskId,
        lease_id: Uuid,
    ) -> Result<(Run, i32)> {
        let (kind, attempts): (String, i32) = sqlx::query_as(
            "select kind, attempts from tasks where id = $1 and lease_id = $2 and state = 'leased' for update",
        )
        .bind(task.uuid())
        .bind(lease_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(StoreError::LeaseLost)?;
        let table = match kind.as_str() {
            "reconcile" => "reconciles",
            "refresh_heads" => "head_refreshes",
            other => return Err(StoreError::Invalid(format!("unknown task kind {other:?}"))),
        };
        let (id,): (Uuid,) = sqlx::query_as(&format!("select id from {table} where task_id = $1"))
            .bind(task.uuid())
            .fetch_one(&mut **tx)
            .await?;
        let run = if kind == "reconcile" {
            Run::Reconcile(id)
        } else {
            Run::Refresh(id)
        };
        Ok((run, attempts))
    }

    /// Finishes a run. A reconcile marks the repositories it did not see as inaccessible — and
    /// only after a complete listing; an incomplete one would mark whatever sat on the pages it
    /// never read.
    pub async fn complete(
        &self,
        task: TaskId,
        lease_id: Uuid,
        complete_listing: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let (run, _) = self.leased_run(&mut tx, task, lease_id).await?;
        match run {
            Run::Refresh(id) => {
                sqlx::query(
                    "update head_refreshes set status = 'succeeded', finished_at = now(), error = null
                     where id = $1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
            Run::Reconcile(id) => {
                if !complete_listing {
                    return Err(StoreError::Invalid(
                        "complete requires a complete listing; report an incomplete one as a failure"
                            .into(),
                    ));
                }
                let (connection_id, watermark): (Uuid, DateTime<Utc>) =
                    sqlx::query_as("select connection_id, watermark from reconciles where id = $1")
                        .bind(id)
                        .fetch_one(&mut *tx)
                        .await?;
                let inaccessible = sqlx::query(
                    "update repositories set status = 'inaccessible', inaccessible_since = now()
                     where connection_id = $1 and status = 'active' and last_seen_at < $2",
                )
                .bind(connection_id)
                .bind(watermark)
                .execute(&mut *tx)
                .await?
                .rows_affected() as i64;
                // Distinct repositories seen by this run: one met on two pages of a shifting
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
                .bind(id)
                .bind(discovered)
                .bind(inaccessible)
                .execute(&mut *tx)
                .await?;
                sqlx::query("update connections set last_reconciled_at = now() where id = $1")
                    .bind(connection_id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        sqlx::query(
            "update tasks set state = 'done', lease_id = null, updated_at = now() where id = $1",
        )
        .bind(task.uuid())
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
        let (run, attempts) = self.leased_run(&mut tx, task, lease_id).await?;
        let (table, id) = match run {
            Run::Reconcile(id) => ("reconciles", id),
            Run::Refresh(id) => ("head_refreshes", id),
        };
        let message: String = message.chars().take(4000).collect();
        let requeue = retryable && attempts < MAX_ATTEMPTS;
        sqlx::query(if requeue {
            "update tasks set state = 'queued', lease_id = null, lease_expires_at = null,
                    last_error = $2, updated_at = now()
             where id = $1"
        } else {
            "update tasks set state = 'failed', lease_id = null, last_error = $2, updated_at = now()
             where id = $1"
        })
        .bind(task.uuid())
        .bind(&message)
        .execute(&mut *tx)
        .await?;
        let run_update = if requeue {
            format!("update {table} set status = 'queued', error = $2 where id = $1")
        } else {
            format!(
                "update {table} set status = 'failed', error = $2, finished_at = now() where id = $1"
            )
        };
        sqlx::query(&run_update)
            .bind(id)
            .bind(&message)
            .execute(&mut *tx)
            .await?;
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
        || p.empty != r.empty
        || p.web_url != r.web_url
}

fn is_commit_id(h: &str) -> bool {
    matches!(h.len(), 40 | 64)
        && h.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn validate_snapshot(r: &RepoSnapshot) -> Result<()> {
    if r.external_id.is_empty() || r.full_name.is_empty() || r.name.is_empty() {
        return Err(StoreError::Invalid(format!(
            "a listed repository needs an external id, a full name and a name: {r:?}"
        )));
    }
    if r.locator.is_empty() {
        return Err(StoreError::Invalid(format!(
            "{}: a listed repository needs a locator",
            r.full_name
        )));
    }
    Ok(())
}
