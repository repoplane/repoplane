//! `repoplane-api.yaml`. Like forgelab's `sandboxes.yaml`, it names secrets and never holds
//! them: every credential is the NAME of an environment variable.

use std::path::Path;

use anyhow::{Context, bail};
use repoplane_flow::TemporalConfig;
use repoplane_proto::secret;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_database_url_env")]
    pub database_url_env: String,
    /// The variable holding the token every `/v1` request must carry: the one the CLI sends.
    #[serde(default = "default_token_env")]
    pub token_env: String,
    #[serde(default)]
    pub temporal: TemporalConfig,
}

fn default_listen() -> String {
    "127.0.0.1:8080".into()
}

fn default_database_url_env() -> String {
    "REPOPLANE_DATABASE_URL".into()
}

fn default_token_env() -> String {
    "REPOPLANE_TOKEN".into()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: default_listen(),
            database_url_env: default_database_url_env(),
            token_env: default_token_env(),
            temporal: TemporalConfig::default(),
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
        for (field, v) in [
            ("database_url_env", &self.database_url_env),
            ("token_env", &self.token_env),
        ] {
            if !secret::valid_env_name(v) {
                bail!(secret::describe_env_name_error(field, v));
            }
        }
        self.temporal.validate()?;
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
        assert_eq!(c.temporal.address, "http://localhost:7233");
        assert_eq!(c.temporal.namespace, "default");
        assert!(serde_yaml_ng::from_str::<Config>("lisen: x").is_err());
        let bad: Config =
            serde_yaml_ng::from_str("token_env: ghp_0123456789abcdefABCDEF0123456789abcd").unwrap();
        assert!(
            bad.validate()
                .unwrap_err()
                .to_string()
                .contains("looks like a token")
        );
    }

    /// The example operators copy says "these are the defaults": it must stay true, and the key
    /// it shows commented out must still be one the file accepts.
    #[test]
    fn the_example_is_the_defaults() {
        let example = include_str!("../../../examples/repoplane-api.yaml");
        let parsed: Config = serde_yaml_ng::from_str(example).unwrap();
        assert_eq!(parsed, Config::default());
        // A file without keys means what no file means.
        assert_eq!(
            serde_yaml_ng::from_str::<Config>("{}").unwrap(),
            Config::default()
        );

        let cloud: Config =
            serde_yaml_ng::from_str(&example.replace("# api_key_env:", "api_key_env:")).unwrap();
        assert_eq!(
            cloud.temporal.api_key_env.as_deref(),
            Some("TEMPORAL_API_KEY")
        );
        cloud.validate().unwrap();
    }
}
