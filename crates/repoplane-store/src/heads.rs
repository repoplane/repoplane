//! Head refreshes: reading the default-branch HEAD of chosen repositories, separately from the
//! inventory. The targets are fixed when the refresh is requested, so what it reads is what was
//! asked for, whatever a reconcile does meanwhile.

use chrono::{DateTime, Utc};
use repoplane_proto::api::{Connection, HeadRefresh, Selector, UNREAD_SHOWN, UnreadRepository};
use repoplane_proto::id::{ConnectionId, HeadRefreshId};
use uuid::Uuid;

use crate::repos::{SelectorArgs, selector_condition};
use crate::{Result, Store, StoreError, parse_status};

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    connection_id: Uuid,
    status: String,
    targeted: i64,
    observed: i64,
    changed: i64,
    unread: i64,
    error: Option<String>,
    requested_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl Row {
    fn into_api(self) -> Result<HeadRefresh> {
        Ok(HeadRefresh {
            id: HeadRefreshId::from_uuid(self.id),
            connection_id: ConnectionId::from_uuid(self.connection_id),
            status: parse_status(&self.status)?,
            targeted: self.targeted,
            observed: self.observed,
            changed: self.changed,
            unread: self.unread,
            unread_repositories: Vec::new(),
            error: self.error,
            requested_at: self.requested_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

impl Store {
    /// Starts reading the HEADs of a connection's active repositories that `select` selects.
    /// With nothing to read, the refresh is finished as it is created.
    pub async fn request_head_refresh(
        &self,
        connection: &Connection,
        select: &Selector,
    ) -> Result<HeadRefresh> {
        let sel = SelectorArgs::new(select)?;
        if !select.topic.is_empty() && !connection.forge.caps().topics {
            return Err(StoreError::Invalid(format!(
                "{} has no repository topics, so connection {:?} cannot be filtered by topic",
                connection.forge, connection.name
            )));
        }
        let mut tx = self.pool.begin().await?;
        let targets: Vec<Uuid> = sqlx::query_scalar(&format!(
            "select id from repositories
             where connection_id = $1 and status = 'active'
               -- Without a locator the worker cannot find a repository again; the next
               -- reconcile writes one (rows recorded before migration 0002 have none).
               and locator <> ''
               and ({})
             order by full_name",
            selector_condition(2)
        ))
        .bind(connection.id.uuid())
        .bind(&sel.topics)
        .bind(&sel.name_like)
        .bind(sel.archived)
        .fetch_all(&mut *tx)
        .await?;
        let id = HeadRefreshId::new();
        if targets.is_empty() {
            sqlx::query(
                "insert into head_refreshes
                     (id, organization_id, connection_id, status, targets, targeted, started_at, finished_at)
                 values ($1, $2, $3, 'succeeded', '{}', 0, now(), now())",
            )
            .bind(id.uuid())
            .bind(self.org.uuid())
            .bind(connection.id.uuid())
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "insert into head_refreshes
                     (id, organization_id, connection_id, status, targets, targeted)
                 values ($1, $2, $3, 'queued', $4, $5)",
            )
            .bind(id.uuid())
            .bind(self.org.uuid())
            .bind(connection.id.uuid())
            .bind(&targets)
            .bind(targets.len() as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.head_refresh(id).await
    }

    pub async fn head_refresh(&self, id: HeadRefreshId) -> Result<HeadRefresh> {
        let row: Option<Row> = sqlx::query_as(
            "select id, connection_id, status, targeted, observed, changed, unread, error,
                    requested_at, started_at, finished_at
             from head_refreshes where organization_id = $1 and id = $2",
        )
        .bind(self.org.uuid())
        .bind(id.uuid())
        .fetch_optional(&self.pool)
        .await?;
        let mut refresh = row
            .ok_or_else(|| StoreError::NotFound(format!("head refresh {id}")))?
            .into_api()?;
        if refresh.unread > 0 {
            let unread: Vec<(String, String)> = sqlx::query_as(
                "select r.full_name, u.reason from head_refresh_unread u
                 join repositories r on r.id = u.repository_id
                 where u.organization_id = $1 and u.head_refresh_id = $2
                 order by r.full_name limit $3",
            )
            .bind(self.org.uuid())
            .bind(id.uuid())
            .bind(UNREAD_SHOWN as i64)
            .fetch_all(&self.pool)
            .await?;
            refresh.unread_repositories = unread
                .into_iter()
                .map(|(full_name, reason)| UnreadRepository { full_name, reason })
                .collect();
        }
        Ok(refresh)
    }
}
