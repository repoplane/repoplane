//! `~/.config/repoplane/contexts.yaml`: which API to talk to, and the NAME of the variable that
//! holds the token for it — shaped like forgelab's `sandboxes.yaml`, and like it, never holding
//! a secret.
//!
//! ```yaml
//! version: 1
//! current: local
//! contexts:
//!   local:
//!     api_url: http://127.0.0.1:8080
//!     token_env: REPOPLANE_TOKEN
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context as _, bail};
use repoplane_proto::secret;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    version: u32,
    current: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, Entry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    api_url: String,
    #[serde(default = "default_token_env")]
    token_env: String,
}

fn default_token_env() -> String {
    "REPOPLANE_TOKEN".into()
}

#[derive(Debug, Clone)]
pub struct Context {
    pub name: String,
    pub api_url: String,
    pub token_env: String,
}

impl Context {
    pub fn token(&self) -> anyhow::Result<String> {
        match std::env::var(&self.token_env) {
            Ok(t) if !t.is_empty() => Ok(t),
            _ => bail!(
                "context {:?}: the environment variable {} is not set (export it with the API's admin token)",
                self.name,
                self.token_env
            ),
        }
    }
}

pub fn path() -> PathBuf {
    if let Some(p) = std::env::var_os("REPOPLANE_CONFIG") {
        return PathBuf::from(p);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("repoplane").join("contexts.yaml")
}

/// The context to use: `--context`, else the file's `current`, else — with no file at all — the
/// local default (`REPOPLANE_API_URL` or http://127.0.0.1:8080, token in `REPOPLANE_TOKEN`).
pub fn resolve(requested: Option<&str>, api_url_override: Option<&str>) -> anyhow::Result<Context> {
    let p = path();
    let mut ctx = match std::fs::read_to_string(&p) {
        Ok(raw) => {
            let f: File =
                serde_yaml_ng::from_str(&raw).with_context(|| format!("read {}", p.display()))?;
            if f.version != 1 {
                bail!(
                    "{}: version {} is not supported (want 1)",
                    p.display(),
                    f.version
                );
            }
            let name = requested
                .map(str::to_string)
                .or(f.current.clone())
                .or_else(|| {
                    (f.contexts.len() == 1).then(|| f.contexts.keys().next().unwrap().clone())
                })
                .with_context(|| {
                    format!("{}: no `current` context; pass --context", p.display())
                })?;
            let e = f.contexts.get(&name).with_context(|| {
                format!(
                    "{}: no context {name:?} (have: {})",
                    p.display(),
                    f.contexts.keys().cloned().collect::<Vec<_>>().join(", ")
                )
            })?;
            Context {
                name,
                api_url: e.api_url.clone(),
                token_env: e.token_env.clone(),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(r) = requested {
                bail!("no context {r:?}: {} does not exist", p.display());
            }
            Context {
                name: "default".into(),
                api_url: std::env::var("REPOPLANE_API_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
                token_env: default_token_env(),
            }
        }
        Err(e) => return Err(e).with_context(|| format!("read {}", p.display())),
    };
    if let Some(u) = api_url_override {
        ctx.api_url = u.to_string();
    }
    if !secret::valid_env_name(&ctx.token_env) {
        bail!(secret::describe_env_name_error("token_env", &ctx.token_env));
    }
    Ok(ctx)
}
