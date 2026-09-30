//! The HTTP surface: `/v1` for clients (admin token), `/v1/worker` for workers (a token per
//! queue). The API never calls a forge; it only records what workers report.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use repoplane_proto::api::{
    ConnectionList, CreateConnection, ErrorBody, Reconcile, RepositoryList,
};
use repoplane_proto::id::{ReconcileId, TaskId};
use repoplane_proto::worker::{
    Complete, Fail, Heartbeat, Lease, LeaseRequest, PageAck, PageUpload,
};
use repoplane_store::{RepoFilter, Store, StoreError};
use serde::Deserialize;

/// A lease lasts this long without news from its worker.
pub const LEASE_TTL_SECS: i64 = 60;
const MAX_LEASE_WAIT: Duration = Duration::from_secs(30);
const MAX_RECONCILE_WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub admin_token: Arc<String>,
    /// queue name → token.
    pub queue_tokens: Arc<BTreeMap<String, String>>,
}

pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route(
            "/connections",
            post(create_connection).get(list_connections),
        )
        .route("/connections/{key}", get(get_connection))
        .route("/connections/{key}/reconciles", post(request_reconcile))
        .route("/reconciles/{id}", get(get_reconcile))
        .route("/repositories", get(list_repositories));
    let worker = Router::new()
        .route("/lease", post(lease))
        .route("/tasks/{id}/heartbeat", post(heartbeat))
        .route("/tasks/{id}/pages", post(upload_page))
        .route("/tasks/{id}/complete", post(complete))
        .route("/tasks/{id}/fail", post(fail));
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .nest("/v1/worker", worker)
        .nest("/v1", public)
        .with_state(state)
}

// --- errors ----------------------------------------------------------------------------

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid", message)
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Invalid(m) => ApiError::new(StatusCode::BAD_REQUEST, "invalid", m),
            StoreError::NotFound(m) => {
                ApiError::new(StatusCode::NOT_FOUND, "not_found", format!("{m} not found"))
            }
            StoreError::Conflict(m) => ApiError::new(StatusCode::CONFLICT, "conflict", m),
            StoreError::LeaseLost => {
                ApiError::new(StatusCode::CONFLICT, "lease_lost", e.to_string())
            }
            StoreError::Schema { .. } => {
                ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "schema", e.to_string())
            }
            other => {
                tracing::error!(error = %other, "store failure");
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "internal error",
                )
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                code: self.code.into(),
                message: self.message,
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

// --- auth ------------------------------------------------------------------------------

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Compares without an early exit, so the time taken says nothing about how much matched.
fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= (a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0)) as usize;
    }
    diff == 0
}

fn unauthorized() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "missing or wrong token",
    )
}

fn admin(s: &AppState, h: &HeaderMap) -> ApiResult<()> {
    match bearer(h) {
        Some(t) if same(t, &s.admin_token) => Ok(()),
        _ => Err(unauthorized()),
    }
}

/// The queue whose token the worker presented.
fn worker_queue(s: &AppState, h: &HeaderMap) -> ApiResult<String> {
    let t = bearer(h).ok_or_else(unauthorized)?;
    let mut found = None;
    for (q, token) in s.queue_tokens.iter() {
        if same(t, token) {
            found = Some(q.clone());
        }
    }
    found.ok_or_else(unauthorized)
}

// --- public ----------------------------------------------------------------------------

async fn readyz(State(s): State<AppState>) -> ApiResult<&'static str> {
    s.store.check_schema().await?;
    Ok("ready")
}

async fn create_connection(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(body): Json<CreateConnection>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    if let Some(q) = &body.queue
        && !s.queue_tokens.contains_key(q)
    {
        return Err(ApiError::bad(format!(
            "queue {q:?} is not configured on this API; configured: {}",
            s.queue_tokens
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let c = s.store.create_connection(&body).await?;
    Ok((StatusCode::CREATED, Json(c)))
}

async fn list_connections(
    State(s): State<AppState>,
    h: HeaderMap,
) -> ApiResult<Json<ConnectionList>> {
    admin(&s, &h)?;
    Ok(Json(ConnectionList {
        items: s.store.connections().await?,
    }))
}

async fn get_connection(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    Ok(Json(s.store.connection(&key).await?))
}

async fn request_reconcile(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    let c = s.store.connection(&key).await?;
    let (r, created) = s.store.request_reconcile(&c).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(r)))
}

#[derive(Deserialize)]
struct WaitQuery {
    wait: Option<String>,
}

/// `30`, `30s` or `1m`.
fn parse_wait(s: &str) -> Result<Duration, ApiError> {
    let (n, unit) = match s.strip_suffix('s') {
        Some(n) => (n, 1),
        None => match s.strip_suffix('m') {
            Some(n) => (n, 60),
            None => (s, 1),
        },
    };
    n.parse::<u64>()
        .map(|n| Duration::from_secs(n * unit))
        .map_err(|_| ApiError::bad(format!("wait {s:?}: want seconds, like 30s")))
}

/// Answers at once, or with `?wait=` holds the request until the run ends or the wait is up.
async fn get_reconcile(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<WaitQuery>,
) -> ApiResult<Json<Reconcile>> {
    admin(&s, &h)?;
    let id: ReconcileId = id
        .parse()
        .map_err(|e: repoplane_proto::id::IdError| ApiError::bad(e.to_string()))?;
    let wait = match q.wait.as_deref() {
        Some(w) => parse_wait(w)?.min(MAX_RECONCILE_WAIT),
        None => Duration::ZERO,
    };
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let r = s.store.reconcile(id).await?;
        if r.status.is_terminal() || tokio::time::Instant::now() + POLL > deadline {
            return Ok(Json(r));
        }
        tokio::time::sleep(POLL).await;
    }
}

#[derive(Deserialize)]
struct RepoQuery {
    connection: Option<String>,
    topic: Option<String>,
    archived: Option<bool>,
    #[serde(default)]
    include_inaccessible: bool,
    cursor: Option<String>,
    limit: Option<i64>,
}

async fn list_repositories(
    State(s): State<AppState>,
    h: HeaderMap,
    Query(q): Query<RepoQuery>,
) -> ApiResult<Json<RepositoryList>> {
    admin(&s, &h)?;
    let connection = match &q.connection {
        Some(key) => {
            let c = s.store.connection(key).await?;
            if q.topic.is_some() && !c.forge.caps().topics {
                return Err(ApiError::bad(format!(
                    "{} has no repository topics, so connection {:?} cannot be filtered by topic",
                    c.forge, c.name
                )));
            }
            Some(c.id)
        }
        None => None,
    };
    let page = s
        .store
        .repositories(&RepoFilter {
            connection,
            topic: q.topic,
            archived: q.archived,
            include_inaccessible: q.include_inaccessible,
            cursor: q.cursor,
            limit: q.limit.unwrap_or(500),
        })
        .await?;
    Ok(Json(RepositoryList {
        items: page.items,
        next_cursor: page.next_cursor,
    }))
}

// --- worker ----------------------------------------------------------------------------

async fn lease(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(req): Json<LeaseRequest>,
) -> ApiResult<Response> {
    let queue = worker_queue(&s, &h)?;
    if req.queue != queue {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("this token serves queue {queue:?}, not {:?}", req.queue),
        ));
    }
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(req.wait_ms).min(MAX_LEASE_WAIT);
    loop {
        if let Some(l) = s.store.lease(&queue, LEASE_TTL_SECS).await? {
            tracing::info!(task = %l.task, queue, "leased");
            return Ok(Json(Lease {
                task: l.task,
                lease_id: l.lease_id,
                ttl_secs: LEASE_TTL_SECS as u64,
                work: l.work,
            })
            .into_response());
        }
        if tokio::time::Instant::now() + POLL > deadline {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        tokio::time::sleep(POLL).await;
    }
}

fn task_id(id: &str) -> ApiResult<TaskId> {
    id.parse()
        .map_err(|e: repoplane_proto::id::IdError| ApiError::bad(e.to_string()))
}

async fn heartbeat(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Json(b): Json<Heartbeat>,
) -> ApiResult<StatusCode> {
    worker_queue(&s, &h)?;
    s.store
        .heartbeat(task_id(&id)?, b.lease_id, LEASE_TTL_SECS)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn upload_page(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Json(b): Json<PageUpload>,
) -> ApiResult<Json<PageAck>> {
    worker_queue(&s, &h)?;
    let out = s
        .store
        .upload_page(task_id(&id)?, b.lease_id, LEASE_TTL_SECS, &b.repos)
        .await?;
    Ok(Json(PageAck {
        recorded: out.recorded,
    }))
}

async fn complete(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Json(b): Json<Complete>,
) -> ApiResult<StatusCode> {
    worker_queue(&s, &h)?;
    s.store
        .complete(task_id(&id)?, b.lease_id, b.complete_listing)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn fail(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Json(b): Json<Fail>,
) -> ApiResult<StatusCode> {
    worker_queue(&s, &h)?;
    s.store
        .fail(task_id(&id)?, b.lease_id, &b.message, b.retryable)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison() {
        assert!(same("abc", "abc"));
        assert!(!same("abc", "abd"));
        assert!(!same("abc", "abcd"));
        assert!(!same("", "a"));
    }

    #[test]
    fn waits() {
        assert_eq!(parse_wait("30s").ok().unwrap(), Duration::from_secs(30));
        assert_eq!(parse_wait("2m").ok().unwrap(), Duration::from_secs(120));
        assert_eq!(parse_wait("5").ok().unwrap(), Duration::from_secs(5));
        assert!(parse_wait("soon").is_err());
    }
}
