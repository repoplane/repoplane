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
}

impl Fake {
    pub fn new(repos: HashMap<String, Listed>) -> Fake {
        Fake {
            repos,
            asked: Mutex::new(Vec::new()),
        }
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
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
        self.asked
            .lock()
            .unwrap()
            .push(format!("heads {}", repos.len()));
        Ok(repos.iter().map(|_| Some("a".repeat(40))).collect())
    }

    async fn get(&self, id: &str) -> Result<Option<Listed>, ForgeError> {
        self.asked.lock().unwrap().push(format!("get {id}"));
        Ok(self.repos.get(id).cloned())
    }
}
