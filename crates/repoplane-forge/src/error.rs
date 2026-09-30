//! What went wrong talking to a forge, classified so the worker can tell "retry later" from
//! "this will never work" — and never carrying the token in the message.

use std::time::Duration;

/// What an HTTP answer means for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    NotFound,
    /// 401, or a forge's sign-in page in place of an answer (Azure DevOps' 203).
    Auth,
    /// 403 that is not a rate limit: the token lacks a permission.
    Forbidden,
    /// 429, or the 403 GitHub uses for the same purpose.
    RateLimited,
    /// 5xx, 408: worth another try.
    Transient,
    Permanent,
}

impl Class {
    pub fn of_status(status: u16) -> Class {
        match status {
            404 | 410 => Class::NotFound,
            401 => Class::Auth,
            403 => Class::Forbidden,
            429 => Class::RateLimited,
            408 | 500 | 502 | 503 | 504 => Class::Transient,
            _ => Class::Permanent,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    #[error("{forge}: {method} {path}: {status} {body}")]
    Status {
        forge: &'static str,
        class: Class,
        status: u16,
        method: String,
        path: String,
        body: String,
    },
    #[error("{forge}: {method} {path}: rate limited, and the wait ({}s) outlasts the budget", wait.as_secs())]
    RateLimitExceeded {
        forge: &'static str,
        method: String,
        path: String,
        wait: Duration,
    },
    #[error("{forge}: {method} {path}: {message}")]
    Transport {
        forge: &'static str,
        method: String,
        path: String,
        message: String,
    },
    #[error("{forge}: {method} {path}: cannot decode the answer: {message}")]
    Decode {
        forge: &'static str,
        method: String,
        path: String,
        message: String,
    },
    /// The forge answered in a way the protocol does not allow: a page that repeats, a cursor
    /// that does not move. Reported, never looped on.
    #[error("{forge}: {message}")]
    Protocol {
        forge: &'static str,
        message: String,
    },
    #[error("{0}")]
    Config(String),
    #[error("cancelled")]
    Cancelled,
}

impl ForgeError {
    /// Worth leasing the task again later: the forge was busy or unreachable, not wrong.
    pub fn retryable(&self) -> bool {
        match self {
            ForgeError::Status { class, .. } => {
                matches!(class, Class::Transient | Class::RateLimited)
            }
            ForgeError::RateLimitExceeded { .. } | ForgeError::Transport { .. } => true,
            ForgeError::Cancelled => true,
            ForgeError::Decode { .. } | ForgeError::Protocol { .. } | ForgeError::Config(_) => {
                false
            }
        }
    }

    pub fn class(&self) -> Option<Class> {
        match self {
            ForgeError::Status { class, .. } => Some(*class),
            _ => None,
        }
    }

    pub fn protocol(forge: &'static str, message: impl Into<String>) -> Self {
        ForgeError::Protocol {
            forge,
            message: message.into(),
        }
    }
}
