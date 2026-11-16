//! How a repoplane process reaches Temporal: a self-hosted server, or Temporal Cloud. Like every
//! other credential in repoplane, the API key is named, never written: `api_key_env` is the NAME
//! of the variable that holds it.

use serde::Deserialize;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, TlsOptions};

use repoplane_proto::secret;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalConfig {
    /// `http://localhost:7233` for a local server; `https://<ns>.<acct>.tmprl.cloud:7233` for
    /// Temporal Cloud, which also needs `api_key_env`. `https://` turns TLS on.
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// The NAME of the variable holding a Temporal Cloud API key. It needs an `https://`
    /// address, or a loopback one: it is never sent in cleartext over the network.
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
        let target = self.target()?;
        if self.api_key_env.is_some() && target.scheme() == "http" && !is_loopback(&target) {
            return Err(TemporalError::Config(format!(
                "address {:?}: an API key would travel in cleartext over http://; use https://",
                self.address
            )));
        }
        Ok(())
    }

    fn target(&self) -> Result<url::Url, TemporalError> {
        let target = url::Url::parse(&self.address).map_err(|e| {
            TemporalError::Config(format!(
                "address {:?}: {e} (want e.g. http://localhost:7233)",
                self.address
            ))
        })?;
        match target.scheme() {
            "http" | "https" => Ok(target),
            other => Err(TemporalError::Config(format!(
                "address {:?}: want http:// or https://, not {other}://",
                self.address
            ))),
        }
    }

    /// How to reach the server. The address's scheme decides TLS: an `https://` address is
    /// verified against the system's trusted roots, which `SSL_CERT_FILE` or `SSL_CERT_DIR`
    /// replace for a private CA. The client does not turn TLS on for an API key by itself.
    fn options(
        &self,
        identity: &str,
        api_key: Option<String>,
    ) -> Result<ConnectionOptions, TemporalError> {
        let target = self.target()?;
        let tls = (target.scheme() == "https").then(TlsOptions::default);
        Ok(ConnectionOptions::new(target)
            .identity(identity.to_string())
            .maybe_api_key(api_key)
            .maybe_tls_options(tls)
            .connect_timeout(std::time::Duration::from_secs(10))
            .build())
    }

    /// A client bound to the configured namespace.
    pub async fn connect(&self, identity: &str) -> Result<Client, TemporalError> {
        self.validate()?;
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
        let options = self.options(identity, api_key)?;
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

/// An address that never leaves the machine, such as a TLS-terminating sidecar's.
fn is_loopback(target: &url::Url) -> bool {
    match target.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(address: &str, api_key_env: Option<&str>) -> TemporalConfig {
        TemporalConfig {
            address: address.into(),
            api_key_env: api_key_env.map(Into::into),
            ..TemporalConfig::default()
        }
    }

    /// TLS follows the address: on for https://, whether or not there is an API key, and off for
    /// http://, which a local server or a sidecar speaks.
    #[test]
    fn https_turns_tls_on() {
        for (address, key, tls) in [
            ("https://acme.a1b2c.tmprl.cloud:7233", Some("KEY"), true),
            ("https://temporal.internal:7233", None, true),
            ("http://localhost:7233", None, false),
            ("http://127.0.0.1:7233", Some("KEY"), false),
        ] {
            let options = config(address, key)
                .options("test", key.map(|_| "value".to_string()))
                .unwrap();
            assert_eq!(options.tls_options.is_some(), tls, "{address}");
        }
    }

    #[test]
    fn an_api_key_never_travels_in_cleartext() {
        let e = config("http://temporal.internal:7233", Some("TEMPORAL_API_KEY"))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("cleartext"), "{e}");
        config("http://[::1]:7233", Some("TEMPORAL_API_KEY"))
            .validate()
            .unwrap();
        config("http://temporal.internal:7233", None)
            .validate()
            .unwrap();
    }

    #[test]
    fn only_http_and_https() {
        let e = config("grpc://temporal.internal:7233", None)
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("want http:// or https://"), "{e}");
    }
}
