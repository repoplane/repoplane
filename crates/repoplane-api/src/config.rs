//! `repoplane-api.yaml`. Like forgelab's `sandboxes.yaml`, it names secrets and never holds
//! them: every credential is the NAME of an environment variable.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use repoplane_proto::secret;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_database_url_env")]
    pub database_url_env: String,
    #[serde(default)]
    pub admin: Admin,
    /// Task queues and the token a worker presents to serve each.
    #[serde(default = "default_queues")]
    pub queues: BTreeMap<String, Queue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admin {
    pub token_env: String,
}

impl Default for Admin {
    fn default() -> Self {
        Admin {
            token_env: "REPOPLANE_ADMIN_TOKEN".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queue {
    pub token_env: String,
}

fn default_listen() -> String {
    "127.0.0.1:8080".into()
}

fn default_database_url_env() -> String {
    "REPOPLANE_DATABASE_URL".into()
}

fn default_queues() -> BTreeMap<String, Queue> {
    BTreeMap::from([(
        "default".to_string(),
        Queue {
            token_env: "REPOPLANE_WORKER_TOKEN".into(),
        },
    )])
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: default_listen(),
            database_url_env: default_database_url_env(),
            admin: Admin::default(),
            queues: default_queues(),
        }
    }
}

impl Config {
    /// Reads the file if it exists; without one, the defaults above apply.
    pub fn load(path: &Path, required: bool) -> anyhow::Result<Config> {
        let cfg = match std::fs::read_to_string(path) {
            Ok(raw) => {
                serde_yaml_ng::from_str(&raw).with_context(|| format!("read {}", path.display()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => Config::default(),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let mut names = vec![
            ("database_url_env", &self.database_url_env),
            ("admin.token_env", &self.admin.token_env),
        ];
        for q in self.queues.values() {
            names.push(("queues.*.token_env", &q.token_env));
        }
        for (field, v) in names {
            if !secret::valid_env_name(v) {
                bail!(secret::describe_env_name_error(field, v));
            }
        }
        if self.queues.is_empty() {
            bail!("queues: at least one queue is needed for a worker to serve");
        }
        Ok(())
    }
}

/// Reads a variable the configuration names. Empty counts as unset.
pub fn read_env(name: &str) -> anyhow::Result<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => bail!("the environment variable {name} is not set"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_strictness() {
        let c: Config = serde_yaml_ng::from_str("listen: 0.0.0.0:9000").unwrap();
        assert_eq!(c.queues["default"].token_env, "REPOPLANE_WORKER_TOKEN");
        assert!(serde_yaml_ng::from_str::<Config>("lisen: x").is_err());
        let bad: Config =
            serde_yaml_ng::from_str("admin: {token_env: ghp_0123456789abcdefABCDEF0123456789abcd}")
                .unwrap();
        assert!(
            bad.validate()
                .unwrap_err()
                .to_string()
                .contains("looks like a token")
        );
    }
}
