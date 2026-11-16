//! `repoplane-api`: owns Postgres, serves `/v1`, and runs the Temporal control worker that hosts
//! the workflows and the activities touching Postgres (ADR 0012). It never calls a forge and
//! never holds a forge credential (ADR 0002, 0003).

mod config;
// The activity macro also generates call markers for workflows, which this binary never names.
#[allow(dead_code)]
mod control;
mod http;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use clap::{Parser, Subcommand};
use repoplane_flow::{
    CheckConnectionWorkflow, LookupsWorkflow, ReadHeadsWorkflow, ReconcileWorkflow,
    RefreshHeadsWorkflow, SyncWorkflow,
};
use repoplane_store::Store;
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

use crate::config::{Config, read_env};

#[derive(Parser)]
#[command(name = "repoplane-api", version, about = "The repoplane API")]
struct Cli {
    /// The configuration file; without one, the documented defaults apply.
    #[arg(long, global = true, default_value = "repoplane-api.yaml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the API. Refuses to start on a schema other than this build's.
    Serve,
    /// Apply pending schema migrations. A deployment step, never done by `serve`.
    Migrate,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let cli = Cli::parse();
    let explicit = std::env::args().any(|a| a == "--config" || a.starts_with("--config="));
    let cfg = Config::load(&cli.config, explicit)?;
    let db_url = read_env(&cfg.database_url_env)?;
    let store = Store::connect(&db_url)
        .await
        .context("connect to Postgres")?;

    match cli.command {
        Command::Migrate => {
            store.migrate().await?;
            println!("ok: schema is current");
            Ok(())
        }
        Command::Serve => {
            store.check_schema().await?;
            let token = read_env(&cfg.token_env)?;
            let temporal = cfg
                .temporal
                .connect(&format!("repoplane-api@{}", hostname()))
                .await?;
            let runtime = Runtime::from_current_tokio(Default::default())
                .map_err(|e| anyhow::anyhow!("temporal runtime: {e}"))?;
            let mut worker = Worker::new(
                &runtime,
                temporal.clone(),
                WorkerOptions::new(repoplane_flow::CONTROL_QUEUE)
                    .register_workflow::<ReconcileWorkflow>()?
                    .register_workflow::<RefreshHeadsWorkflow>()?
                    .register_workflow::<LookupsWorkflow>()?
                    .register_workflow::<ReadHeadsWorkflow>()?
                    .register_workflow::<SyncWorkflow>()?
                    .register_workflow::<CheckConnectionWorkflow>()?
                    .register_activities(control::ControlActivities {
                        store: store.clone(),
                    })
                    .build(),
            )
            .map_err(|e| anyhow::anyhow!("temporal worker: {e}"))?;
            let stop_worker = worker.shutdown_handle();

            let state = http::AppState {
                store,
                token: Arc::new(token),
                temporal,
            };
            let listener = tokio::net::TcpListener::bind(&cfg.listen)
                .await
                .with_context(|| format!("listen on {}", cfg.listen))?;
            tracing::info!(addr = %listener.local_addr()?, queue = repoplane_flow::CONTROL_QUEUE, "serving");

            // One stop for both: a signal ends the HTTP server, which tells the worker to finish
            // its activities and stop. The worker's future is not Send, so the two share this task.
            // A worker that stops by itself (a namespace that fails validation, a rotated key)
            // stops the HTTP server too, and the process exits with an error: serving on with no
            // worker would leave every run queued, with nothing saying why.
            let worker_down = Arc::new(tokio::sync::Notify::new());
            let signalled = Arc::new(AtomicBool::new(false));
            let serve = async {
                let (down, signalled) = (worker_down.clone(), signalled.clone());
                let r = axum::serve(listener, http::router(state))
                    .with_graceful_shutdown(async move {
                        tokio::select! {
                            _ = shutdown() => signalled.store(true, Ordering::SeqCst),
                            _ = down.notified() => {}
                        }
                    })
                    .await;
                stop_worker();
                r
            };
            let work = async {
                let r = worker.run().await;
                worker_down.notify_one();
                r
            };
            let (served, worked) = tokio::join!(serve, work);
            served?;
            worked.map_err(|e| anyhow::anyhow!("temporal worker: {e}"))?;
            if !signalled.load(Ordering::SeqCst) {
                anyhow::bail!("the Temporal worker stopped by itself; exiting so the API restarts");
            }
            Ok(())
        }
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| std::process::id().to_string())
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
