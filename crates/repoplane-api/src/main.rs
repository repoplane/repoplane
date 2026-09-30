//! `repoplane-api`: owns Postgres and serves `/v1` and `/v1/worker`. It never calls a forge and
//! never holds a forge credential (ADR 0002, 0003).

mod config;
mod http;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use repoplane_store::Store;

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
            let admin_token = read_env(&cfg.admin.token_env)?;
            let mut queue_tokens = BTreeMap::new();
            for (q, spec) in &cfg.queues {
                queue_tokens.insert(q.clone(), read_env(&spec.token_env)?);
            }
            let state = http::AppState {
                store,
                admin_token: Arc::new(admin_token),
                queue_tokens: Arc::new(queue_tokens),
            };
            let listener = tokio::net::TcpListener::bind(&cfg.listen)
                .await
                .with_context(|| format!("listen on {}", cfg.listen))?;
            tracing::info!(addr = %listener.local_addr()?, "serving");
            axum::serve(listener, http::router(state))
                .with_graceful_shutdown(shutdown())
                .await?;
            Ok(())
        }
    }
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
