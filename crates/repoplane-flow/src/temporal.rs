//! How a repoplane process reaches Temporal: a self-hosted server, or Temporal Cloud. Like every
//! other credential in repoplane, the API key is named, never written: `api_key_env` is the NAME
//! of the variable that holds it.

use serde::Deserialize;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};

use repoplane_proto::secret;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalConfig {
    /// `http://localhost:7233` for a local server; `https://<ns>.<acct>.tmprl.cloud:7233` for
    /// Temporal Cloud, which also needs `api_key_env`.
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// The NAME of the variable holding a Temporal Cloud API key. Setting it turns TLS on.
    #[serde(default)]
    pub api_key_env: Option<String>,
}

fn default_address() -> String {
    "http://localhost:7233".into()
}

fn default_namespace() -> String {
    "default".into()
}

impl Default for TemporalConfig {
    fn default() -> Self {
        TemporalConfig {
            address: default_address(),
            namespace: default_namespace(),
            api_key_env: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TemporalError {
    #[error("temporal: {0}")]
    Config(String),
    #[error("temporal: cannot connect to {address}: {message}")]
    Connect { address: String, message: String },
}

impl TemporalConfig {
    pub fn validate(&self) -> Result<(), TemporalError> {
        if let Some(name) = &self.api_key_env
            && !secret::valid_env_name(name)
        {
            return Err(TemporalError::Config(secret::describe_env_name_error(
                "temporal.api_key_env",
                name,
            )));
        }
        url::Url::parse(&self.address).map_err(|e| {
            TemporalError::Config(format!(
                "address {:?}: {e} (want e.g. http://localhost:7233)",
                self.address
            ))
        })?;
        Ok(())
    }

    /// A client bound to the configured namespace.
    pub async fn connect(&self, identity: &str) -> Result<Client, TemporalError> {
        self.validate()?;
        let target = url::Url::parse(&self.address).expect("validated");
        let api_key = match &self.api_key_env {
            Some(name) => Some(
                std::env::var(name)
                    .ok()
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| {
                        TemporalError::Config(format!("the environment variable {name} is not set"))
                    })?,
            ),
            None => None,
        };
        let options = ConnectionOptions::new(target)
            .identity(identity.to_string())
            .maybe_api_key(api_key)
            .connect_timeout(std::time::Duration::from_secs(10))
            .build();
        let connection =
            Connection::connect(options)
                .await
                .map_err(|e| TemporalError::Connect {
                    address: self.address.clone(),
                    message: e.to_string(),
                })?;
        Client::new(
            connection,
            ClientOptions::new(self.namespace.clone()).build(),
        )
        .map_err(|e| TemporalError::Connect {
            address: self.address.clone(),
            message: e.to_string(),
        })
    }
}
