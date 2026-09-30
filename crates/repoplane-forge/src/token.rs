//! A forge token. It never prints: `Debug` and `Display` are redacted, and `scrub` removes it
//! from any text headed for a log, the API or an error message — forges have been seen to echo
//! a token back in an error body.

use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct Token(Arc<str>);

impl Token {
    pub fn new(value: impl Into<String>) -> Result<Token, crate::ForgeError> {
        let v: String = value.into();
        let v = v.trim().to_string();
        if v.is_empty() {
            return Err(crate::ForgeError::Config("the token is empty".into()));
        }
        Ok(Token(v.into()))
    }

    /// Reads the variable a connection names, in this process's environment.
    pub fn from_env(name: &str) -> Result<Token, crate::ForgeError> {
        match std::env::var(name) {
            Ok(v) if !v.trim().is_empty() => Token::new(v),
            _ => Err(crate::ForgeError::Config(format!(
                "the environment variable {name} is not set in this worker"
            ))),
        }
    }

    /// The secret, for building an Authorization header and nothing else.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// `text` with every occurrence of the token, and of its base64 forms, replaced.
    pub fn scrub(&self, text: &str) -> String {
        use base64::Engine as _;
        let mut out = text.replace(&*self.0, "[redacted]");
        let b64 = base64::engine::general_purpose::STANDARD;
        for form in [b64.encode(format!(":{}", self.0)), b64.encode(&*self.0)] {
            out = out.replace(&form, "[redacted]");
        }
        out
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token([redacted])")
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_prints() {
        let t = Token::new("s3cr3t-value").unwrap();
        assert_eq!(format!("{t:?} {t}"), "Token([redacted]) [redacted]");
        assert_eq!(
            t.scrub("bad token s3cr3t-value in [sha: s3cr3t-value]"),
            "bad token [redacted] in [sha: [redacted]]"
        );
        use base64::Engine as _;
        let basic = base64::engine::general_purpose::STANDARD.encode(":s3cr3t-value");
        assert!(!t.scrub(&format!("Basic {basic}")).contains(&basic));
    }
}
