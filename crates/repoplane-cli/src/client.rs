//! The `/v1` client. The CLI talks to the API and to nothing else: no database, no forge.

use std::time::Duration;

use anyhow::{Context as _, bail};
use repoplane_proto::api::{
    Connection, ConnectionList, CreateConnection, ErrorBody, Reconcile, RepositoryList,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::context::Context;

pub struct Client {
    http: reqwest::Client,
    base: String,
    token: String,
}

#[derive(Default, Serialize)]
pub struct RepoQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub include_inaccessible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Client {
    pub fn new(ctx: &Context) -> anyhow::Result<Client> {
        Ok(Client {
            http: reqwest::Client::builder()
                .user_agent(concat!("repoplane/", env!("CARGO_PKG_VERSION")))
                .build()?,
            base: format!("{}/v1", ctx.api_url.trim_end_matches('/')),
            token: ctx.token()?,
        })
    }

    pub async fn create_connection(&self, body: &CreateConnection) -> anyhow::Result<Connection> {
        self.send(self.http.post(self.url("/connections")).json(body))
            .await
    }

    pub async fn connections(&self) -> anyhow::Result<ConnectionList> {
        self.send(self.http.get(self.url("/connections"))).await
    }

    pub async fn connection(&self, key: &str) -> anyhow::Result<Connection> {
        self.send(
            self.http
                .get(self.url(&format!("/connections/{}", seg(key)))),
        )
        .await
    }

    pub async fn reconcile(&self, key: &str) -> anyhow::Result<Reconcile> {
        self.send(
            self.http
                .post(self.url(&format!("/connections/{}/reconciles", seg(key)))),
        )
        .await
    }

    /// The run's state; with `wait`, held by the API until the run ends or the wait is up.
    pub async fn reconcile_status(
        &self,
        id: &str,
        wait: Option<Duration>,
    ) -> anyhow::Result<Reconcile> {
        let mut req = self.http.get(self.url(&format!("/reconciles/{}", seg(id))));
        if let Some(w) = wait {
            req = req
                .query(&[("wait", format!("{}s", w.as_secs()))])
                .timeout(w + Duration::from_secs(15));
        }
        self.send(req).await
    }

    pub async fn repositories(&self, q: &RepoQuery) -> anyhow::Result<RepositoryList> {
        self.send(self.http.get(self.url("/repositories")).query(q))
            .await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn send<T: DeserializeOwned>(&self, req: reqwest::RequestBuilder) -> anyhow::Result<T> {
        let resp = req
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("reach the API at {}", self.base))?;
        let status = resp.status();
        if status.is_success() {
            return resp.json().await.context("read the API's answer");
        }
        match resp.json::<ErrorBody>().await {
            Ok(b) => bail!("{}", b.message),
            Err(_) => bail!("the API answered {status}"),
        }
    }
}

fn seg(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
