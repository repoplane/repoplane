//! `repoplane-worker`: a Temporal activity worker for the forge activities, on one queue. It
//! holds the forge tokens (read from the variables each connection names), and talks only to
//! Temporal and to forges: no database, no repoplane API, no inbound connection (ADR 0001).

#[cfg(test)]
mod fake;
// The activity macro also generates call markers for workflows, which this binary never names.
#[allow(dead_code)]
mod forge;

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Parser;
use repoplane_flow::TemporalConfig;
use repoplane_forge::ReqwestTransport;
use serde::Deserialize;
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[derive(Parser)]
#[command(name = "repoplane-worker", version, about = "The repoplane worker")]
struct Cli {
    /// The configuration file; without one, defaults and flags apply.
    #[arg(long, default_value = "repoplane-worker.yaml")]
    config: PathBuf,
    /// Overrides `queue`.
    #[arg(long)]
    queue: Option<String>,
    /// Overrides `temporal.address`.
    #[arg(long)]
    temporal_address: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// The task queue this worker serves: the `queue` of the connections it runs for.
    #[serde(default = "default_queue")]
    queue: String,
    /// Forge activities run at once: the only bound on fan-out. Workflows schedule every page,
    /// lookup and HEAD read at once, and Temporal hands this worker as many as it has slots.
    /// A HEAD read is one `git ls-remote`, a process and a TLS handshake that mostly waits on
    /// the network, so it takes many in flight to go fast. Median of three syncs, HEADs only
    /// (2026-10-06, debug build):
    ///
    /// | slots | GitHub, 2,139 | GitLab, 599 | Azure DevOps, 2,144 |
    /// |---|---|---|---|
    /// | 32 | 39.1 s | 16.0 s | 18.7 s |
    /// | 64 | 20.9 s | 8.7 s | 18.6 s |
    /// | 128 | 17.2 s | | |
    ///
    /// 64 takes most of the gain and stays under GitHub's 100 concurrent requests per token,
    /// which the API calls here (listing pages, lookups by id) count against; 128 could pass it
    /// when many lookups run at once. Azure DevOps answers no faster above 32.
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default)]
    temporal: TemporalConfig,
}

fn default_queue() -> String {
    "default".into()
}
fn default_concurrency() -> usize {
    64
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
    if let Some(q) = &cli.queue {
        cfg.queue = q.clone();
    }
    if let Some(a) = &cli.temporal_address {
        cfg.temporal.address = a.clone();
    }
    if cfg.concurrency == 0 {
        bail!("concurrency must be at least 1");
    }
    cfg.temporal.validate()?;
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
    let identity = format!(
        "repoplane-worker@{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| std::process::id().to_string())
    );
    let client = cfg.temporal.connect(&identity).await?;
    let runtime = Runtime::from_current_tokio(Default::default())
        .map_err(|e| anyhow::anyhow!("temporal runtime: {e}"))?;
    let tuner = TunerHolder::builder()
        .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(1))
        .activity_task_slot_supplier(FixedSizeSlotSupplier::new(cfg.concurrency))
        .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(1))
        .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
        .build();
    let mut worker = Worker::new(
        &runtime,
        client,
        WorkerOptions::new(cfg.queue.clone())
            .register_activities(forge::ForgeActivities {
                transport: ReqwestTransport::new()?,
            })
            .tuner(tuner)
            .build(),
    )
    .map_err(|e| anyhow::anyhow!("temporal worker: {e}"))?;
    let stop = worker.shutdown_handle();
    tracing::info!(queue = %cfg.queue, temporal = %cfg.temporal.address, concurrency = cfg.concurrency, "worker started");
    let (_, ran) = tokio::join!(
        async {
            shutdown().await;
            tracing::info!("shutting down: finishing the activities in flight");
            stop();
        },
        worker.run()
    );
    ran.map_err(|e| anyhow::anyhow!("temporal worker: {e}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The example operators copy shows the defaults: it must stay true, and the key it shows
    /// commented out must still be one the file accepts.
    #[test]
    fn the_example_is_the_defaults() {
        let example = include_str!("../../../examples/repoplane-worker.yaml");
        let defaults: Config = serde_yaml_ng::from_str("{}").unwrap();
        assert_eq!(
            serde_yaml_ng::from_str::<Config>(example).unwrap(),
            defaults
        );

        let cloud: Config =
            serde_yaml_ng::from_str(&example.replace("# api_key_env:", "api_key_env:")).unwrap();
        assert_eq!(
            cloud.temporal.api_key_env.as_deref(),
            Some("TEMPORAL_API_KEY")
        );
        cloud.temporal.validate().unwrap();
    }
}
