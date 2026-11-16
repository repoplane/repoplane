//! A forge for unit tests: its truth is a map from id to repository.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use repoplane_forge::{ForgeError, Listed, Lister, Page};
use repoplane_proto::ForgeKind;
use repoplane_proto::inventory::RepoSnapshot;

pub struct Fake {
    repos: HashMap<String, Listed>,
    asked: Mutex<Vec<String>>,
    /// Repositories whose HEAD read fails, and whether that failure is retryable.
    failing: HashMap<String, bool>,
    /// Repositories the forge will not let the token read (a 403).
    forbidden: std::collections::HashSet<String>,
}

impl Fake {
    pub fn new(repos: HashMap<String, Listed>) -> Fake {
        Fake {
            repos,
            asked: Mutex::new(Vec::new()),
            failing: HashMap::new(),
            forbidden: Default::default(),
        }
    }

    /// Makes the forge refuse the token `id`'s HEAD, and only that one.
    pub fn forbidden(mut self, id: &str) -> Fake {
        self.forbidden.insert(id.to_string());
        self
    }

    /// Makes reading `id`'s HEAD fail, retryably or not.
    pub fn failing(mut self, id: &str, retryable: bool) -> Fake {
        self.failing.insert(id.to_string(), retryable);
        self
    }

    /// What it was asked, in order: `get <id>`, `heads <n>`.
    pub fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }
}

pub fn listed(id: &str, name: &str) -> Listed {
    Listed {
        snapshot: RepoSnapshot {
            external_id: id.into(),
            full_name: format!("acme/{name}"),
            name: name.into(),
            default_branch: Some("main".into()),
            archived: false,
            topics: Some(vec![]),
            locator: format!("acme/{name}"),
            empty: false,
            web_url: None,
        },
    }
}

#[async_trait]
impl Lister for Fake {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Forgejo
    }

    async fn page(&self, _cursor: Option<&str>) -> Result<Page, ForgeError> {
        self.asked.lock().unwrap().push("page".into());
        Ok(Page {
            repos: self.repos.values().cloned().collect(),
            next: None,
            rest: None,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Vec<Result<Option<String>, ForgeError>> {
        self.asked
            .lock()
            .unwrap()
            .push(format!("heads {}", repos.len()));
        repos
            .iter()
            .map(|r| match self.failing.get(&r.snapshot.external_id) {
                _ if self.forbidden.contains(&r.snapshot.external_id) => Err(ForgeError::Status {
                    forge: "fake",
                    class: repoplane_forge::Class::Forbidden,
                    status: 403,
                    method: "git ls-remote".into(),
                    path: "/".into(),
                    body: "You are not allowed to download code from this project.".into(),
                }),
                Some(true) => Err(ForgeError::Transport {
                    forge: "fake",
                    method: "git ls-remote".into(),
                    path: "/".into(),
                    message: "connection reset".into(),
                }),
                Some(false) => Err(ForgeError::Config("the token was refused".into())),
                None => Ok(Some("a".repeat(40))),
            })
            .collect()
    }

    async fn get(&self, id: &str) -> Result<Option<Listed>, ForgeError> {
        self.asked.lock().unwrap().push(format!("get {id}"));
        Ok(self.repos.get(id).cloned())
    }
}
