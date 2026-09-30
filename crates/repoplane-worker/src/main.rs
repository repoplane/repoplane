//! `repoplane-worker`: leases tasks from the API over outbound HTTPS and talks to forges. It
//! holds the forge tokens (read from the variables each connection names) and never opens a
//! database or accepts a connection (ADR 0001, 0003, 0008).

mod api;
#[cfg(test)]
mod fake;
mod heads;
mod reconcile;
mod task;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;
use repoplane_forge::ReqwestTransport;
use repoplane_proto::secret;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "repoplane-worker", version, about = "The repoplane worker")]
struct Cli {
    /// The configuration file; without one, defaults and flags apply.
    #[arg(long, default_value = "repoplane-worker.yaml")]
    config: PathBuf,
    /// Overrides `api_url`.
    #[arg(long)]
    api_url: Option<String>,
    /// Overrides `queue`.
    #[arg(long)]
    queue: Option<String>,
    /// Overrides `concurrency`.
    #[arg(long)]
    concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "default_api_url")]
    api_url: String,
    #[serde(default = "default_queue")]
    queue: String,
    /// The NAME of the variable holding this worker's token for its queue.
    #[serde(default = "default_token_env")]
    token_env: String,
    /// Tasks run at once.
    #[serde(default = "default_concurrency")]
    concurrency: usize,
}

fn default_api_url() -> String {
    "http://127.0.0.1:8080".into()
}
fn default_queue() -> String {
    "default".into()
}
fn default_token_env() -> String {
    "REPOPLANE_WORKER_TOKEN".into()
}
fn default_concurrency() -> usize {
    4
}

fn load(cli: &Cli) -> anyhow::Result<Config> {
    let explicit = std::env::args().any(|a| a == "--config" || a.starts_with("--config="));
    let mut cfg: Config = match std::fs::read_to_string(&cli.config) {
        Ok(raw) => serde_yaml_ng::from_str(&raw)
            .with_context(|| format!("read {}", cli.config.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !explicit => {
            serde_yaml_ng::from_str("{}").expect("defaults")
        }
        Err(e) => return Err(e).with_context(|| format!("read {}", cli.config.display())),
    };
    if let Some(u) = &cli.api_url {
        cfg.api_url = u.clone();
    }
    if let Some(q) = &cli.queue {
        cfg.queue = q.clone();
    }
    if let Some(c) = cli.concurrency {
        cfg.concurrency = c;
    }
    if !secret::valid_env_name(&cfg.token_env) {
        bail!(secret::describe_env_name_error("token_env", &cfg.token_env));
    }
    if cfg.concurrency == 0 {
        bail!("concurrency must be at least 1");
    }
    Ok(cfg)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let cfg = load(&cli)?;
    let token = match std::env::var(&cfg.token_env) {
        Ok(t) if !t.is_empty() => t,
        _ => bail!("the environment variable {} is not set", cfg.token_env),
    };
    let api = Arc::new(api::Api::new(&cfg.api_url, &token)?);
    let transport = ReqwestTransport::new()?;
    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown().await;
            tracing::info!("shutting down: no new leases, cancelling tasks in flight");
            cancel.cancel();
        });
    }
    tracing::info!(api = %cfg.api_url, queue = %cfg.queue, concurrency = cfg.concurrency, "worker started");

    let mut running = JoinSet::new();
    let mut backoff = Duration::from_millis(500);
    while !cancel.is_cancelled() {
        while running.len() >= cfg.concurrency {
            running.join_next().await;
        }
        let leased = tokio::select! {
            r = api.lease(&cfg.queue, Duration::from_secs(20)) => r,
            _ = cancel.cancelled() => break,
        };
        match leased {
            Ok(Some(lease)) => {
                backoff = Duration::from_millis(500);
                let (api, transport, cancel) = (api.clone(), transport.clone(), cancel.clone());
                running.spawn(async move { task::run(api, transport, lease, cancel).await });
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("lease failed, retrying in {}ms: {e:#}", backoff.as_millis());
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = cancel.cancelled() => break,
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    while running.join_next().await.is_some() {}
    Ok(())
}

async fn shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
