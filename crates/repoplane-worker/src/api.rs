//! The worker's side of `/v1/worker`: outbound HTTPS only.

use std::time::Duration;

use anyhow::Context;
use repoplane_proto::api::ErrorBody;
use repoplane_proto::id::TaskId;
use repoplane_proto::worker::{
    Complete, Fail, Heartbeat, Lease, LeaseRequest, PageAck, PageUpload, Unseen, UnseenRequest,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub struct Api {
    http: reqwest::Client,
    base: String,
    token: String,
}

/// The API handed this task to another worker; stop working on it.
#[derive(Debug)]
pub struct LeaseLost;

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lease lost: the task was handed to another worker")
    }
}

impl std::error::Error for LeaseLost {}

impl Api {
    pub fn new(base: &str, token: &str) -> anyhow::Result<Api> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("repoplane-worker/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Api {
            http,
            base: format!("{}/v1/worker", base.trim_end_matches('/')),
            token: token.to_string(),
        })
    }

    /// Waits up to `wait` for a task.
    pub async fn lease(&self, queue: &str, wait: Duration) -> anyhow::Result<Option<Lease>> {
        let resp = self
            .http
            .post(format!("{}/lease", self.base))
            .bearer_auth(&self.token)
            .timeout(wait + Duration::from_secs(15))
            .json(&LeaseRequest {
                queue: queue.to_string(),
                wait_ms: wait.as_millis() as u64,
            })
            .send()
            .await
            .context("lease")?;
        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        Ok(Some(read(resp).await?))
    }

    pub async fn heartbeat(&self, task: TaskId, lease_id: Uuid) -> anyhow::Result<()> {
        self.post_empty(task, "heartbeat", &Heartbeat { lease_id })
            .await
    }

    pub async fn upload(&self, task: TaskId, body: &PageUpload) -> anyhow::Result<PageAck> {
        let resp = self
            .http
            .post(format!("{}/tasks/{task}/pages", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(60))
            .json(body)
            .send()
            .await
            .context("upload page")?;
        read(resp).await
    }

    pub async fn unseen(&self, task: TaskId, lease_id: Uuid) -> anyhow::Result<Unseen> {
        let resp = self
            .http
            .post(format!("{}/tasks/{task}/unseen", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(60))
            .json(&UnseenRequest { lease_id })
            .send()
            .await
            .context("unseen")?;
        read(resp).await
    }

    pub async fn complete(&self, task: TaskId, body: &Complete) -> anyhow::Result<()> {
        self.post_empty(task, "complete", body).await
    }

    pub async fn fail(&self, task: TaskId, body: &Fail) -> anyhow::Result<()> {
        self.post_empty(task, "fail", body).await
    }

    async fn post_empty(
        &self,
        task: TaskId,
        what: &str,
        body: &impl Serialize,
    ) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/tasks/{task}/{what}", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(30))
            .json(body)
            .send()
            .await
            .with_context(|| what.to_string())?;
        if resp.status().is_success() {
            return Ok(());
        }
        Err(error(resp).await)
    }
}

async fn read<T: DeserializeOwned>(resp: reqwest::Response) -> anyhow::Result<T> {
    if !resp.status().is_success() {
        return Err(error(resp).await);
    }
    Ok(resp.json().await?)
}

async fn error(resp: reqwest::Response) -> anyhow::Error {
    let status = resp.status();
    match resp.json::<ErrorBody>().await {
        Ok(b) if b.code == "lease_lost" => LeaseLost.into(),
        Ok(b) => anyhow::anyhow!("API answered {status}: {} ({})", b.message, b.code),
        Err(_) => anyhow::anyhow!("API answered {status}"),
    }
}

pub fn is_lease_lost(e: &anyhow::Error) -> bool {
    e.downcast_ref::<LeaseLost>().is_some()
}
