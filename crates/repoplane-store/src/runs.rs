//! What a run writes, step by step. Temporal decides when a step runs and retries it; these
//! functions only say what each step does to the product state, so each is written to be safe
//! to repeat: an activity can run again after its transaction committed but before Temporal
//! heard back.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use repoplane_proto::id::{HeadRefreshId, ReconcileId};
use repoplane_proto::inventory::{HeadTarget, ObservedHead, RepoSnapshot, UnseenRepo};
use uuid::Uuid;

use crate::{Result, Store, StoreError};

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

impl Store {
    // --- reconcile ---------------------------------------------------------------------------

    /// Marks the run running and fixes its watermark. Repeating it keeps the first watermark.
    pub async fn start_reconcile(&self, reconcile: ReconcileId) -> Result<()> {
        let n = sqlx::query(
            "update reconciles set status = 'running', watermark = coalesce(watermark, now()),
                    started_at = coalesce(started_at, now())
             where id = $1 and status in ('queued', 'running')",
        )
        .bind(reconcile.uuid())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(StoreError::Invalid(format!(
                "reconcile {reconcile} is not queued or running"
            )));
        }
        Ok(())
    }

    /// Records one page of a listing, or the repositories a lookup found still there. A
    /// repository is *recorded* when it is new, changed, or back from inaccessible; every one is
    /// stamped seen at the run's watermark.
    ///
    /// A listing says nothing about HEADs, so they are left alone — except when the default
    /// branch changed, where the recorded HEAD described another branch and is cleared.
    pub async fn record_page(&self, reconcile: ReconcileId, repos: &[RepoSnapshot]) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let (reconcile_id, connection_id, watermark): (Uuid, Uuid, DateTime<Utc>) = sqlx::query_as(
            "select id, connection_id, watermark from reconciles
             where id = $1 and status = 'running' and watermark is not null",
        )
        .bind(reconcile.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::Invalid(format!("reconcile {reconcile} is not running")))?;

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
        Ok(recorded)
    }

    /// The repositories the run has not seen: active in the inventory, and not stamped with its
    /// watermark. The workflow looks each up by id before anything is marked.
    pub async fn unseen(&self, reconcile: ReconcileId) -> Result<Vec<UnseenRepo>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "select r.external_id, r.full_name from repositories r
             join reconciles rc on rc.connection_id = r.connection_id
             where rc.id = $1 and r.status = 'active' and r.last_seen_at < rc.watermark
             order by r.full_name",
        )
        .bind(reconcile.uuid())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(external_id, full_name)| UnseenRepo {
                external_id,
                full_name,
            })
            .collect())
    }

    /// Closes a run whose listing was complete and whose unseen repositories were all looked
    /// up: what it still has not seen is gone, and is marked inaccessible. Repeating it marks
    /// nothing more.
    pub async fn finish_reconcile(&self, reconcile: ReconcileId) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(Uuid, DateTime<Utc>, String)> = sqlx::query_as(
            "select connection_id, watermark, status from reconciles where id = $1 for update",
        )
        .bind(reconcile.uuid())
        .fetch_optional(&mut *tx)
        .await?;
        let (connection_id, watermark, status) =
            row.ok_or_else(|| StoreError::NotFound(format!("reconcile {reconcile}")))?;
        if status == "succeeded" {
            return Ok(());
        }
        if status != "running" {
            return Err(StoreError::Invalid(format!(
                "reconcile {reconcile} is {status}"
            )));
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
        // Distinct repositories seen by this run: one met on two pages of a shifting listing
        // counts once.
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
        .bind(reconcile.uuid())
        .bind(discovered)
        .bind(inaccessible)
        .execute(&mut *tx)
        .await?;
        sqlx::query("update connections set last_reconciled_at = now() where id = $1")
            .bind(connection_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Records why a run failed. Nothing is marked inaccessible.
    pub async fn fail_reconcile(&self, reconcile: ReconcileId, message: &str) -> Result<()> {
        let message: String = message.chars().take(4000).collect();
        sqlx::query(
            "update reconciles set status = 'failed', error = $2, finished_at = now()
             where id = $1 and status <> 'succeeded'",
        )
        .bind(reconcile.uuid())
        .bind(&message)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- head refresh --------------------------------------------------------------------------

    /// Marks the refresh running and returns its targets as the inventory holds them now: a
    /// repository gone inaccessible since the refresh was requested, or one with no locator
    /// yet, is not read.
    pub async fn start_refresh(&self, refresh: HeadRefreshId) -> Result<Vec<HeadTarget>> {
        let n = sqlx::query(
            "update head_refreshes set status = 'running', started_at = coalesce(started_at, now())
             where id = $1 and status in ('queued', 'running')",
        )
        .bind(refresh.uuid())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(StoreError::Invalid(format!(
                "head refresh {refresh} is not queued or running"
            )));
        }
        let targets: Vec<TargetRow> = sqlx::query_as(
            "select r.external_id, r.full_name, r.name, r.default_branch, r.archived,
                    r.topics, r.empty, r.web_url, r.locator
             from head_refreshes h join repositories r on r.id = any(h.targets)
             where h.id = $1 and r.status = 'active' and r.locator <> ''
             order by r.full_name",
        )
        .bind(refresh.uuid())
        .fetch_all(&self.pool)
        .await?;
        Ok(targets
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
            .collect())
    }

    /// Records HEADs a refresh read, and returns how many changed.
    ///
    /// Only the refresh's own targets are written, and only while still active. A HEAD read
    /// from a branch that is no longer the default (a reconcile recorded the change while the
    /// refresh ran) describes the old branch and is dropped. Each target counts once per
    /// refresh, repeats included: *observed* when first recorded, *changed* when that differs
    /// from what was recorded before the refresh.
    pub async fn record_heads(
        &self,
        refresh: HeadRefreshId,
        heads: &[ObservedHead],
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let (refresh_id, started_at): (Uuid, DateTime<Utc>) = sqlx::query_as(
            "select id, started_at from head_refreshes where id = $1 and status = 'running'",
        )
        .bind(refresh.uuid())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::Invalid(format!("head refresh {refresh} is not running")))?;
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

    pub async fn finish_refresh(&self, refresh: HeadRefreshId) -> Result<()> {
        sqlx::query(
            "update head_refreshes set status = 'succeeded', finished_at = now(), error = null
             where id = $1 and status = 'running'",
        )
        .bind(refresh.uuid())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_refresh(&self, refresh: HeadRefreshId, message: &str) -> Result<()> {
        let message: String = message.chars().take(4000).collect();
        sqlx::query(
            "update head_refreshes set status = 'failed', error = $2, finished_at = now()
             where id = $1 and status <> 'succeeded'",
        )
        .bind(refresh.uuid())
        .bind(&message)
        .execute(&self.pool)
        .await?;
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
