//! The forges repoplane speaks to, and what each one can and cannot say about a repository.
//! The differences are modelled rather than hidden: a forge without topics answers "this forge
//! has no topics", never an empty list that reads as "this repository has none".

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ForgeKind {
    Forgejo,
    Github,
    Gitlab,
    Azuredevops,
}

impl ForgeKind {
    pub const ALL: [ForgeKind; 4] = [
        ForgeKind::Forgejo,
        ForgeKind::Github,
        ForgeKind::Gitlab,
        ForgeKind::Azuredevops,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ForgeKind::Forgejo => "forgejo",
            ForgeKind::Github => "github",
            ForgeKind::Gitlab => "gitlab",
            ForgeKind::Azuredevops => "azuredevops",
        }
    }

    /// The base URL used when a connection names none. Forgejo is always self-hosted.
    pub fn default_base_url(self) -> Option<&'static str> {
        match self {
            ForgeKind::Forgejo => None,
            ForgeKind::Github => Some("https://github.com"),
            ForgeKind::Gitlab => Some("https://gitlab.com"),
            ForgeKind::Azuredevops => Some("https://dev.azure.com"),
        }
    }

    pub fn caps(self) -> Caps {
        match self {
            ForgeKind::Forgejo | ForgeKind::Github => Caps {
                topics: true,
                nested_namespaces: false,
                archived: Archived::Archived,
            },
            ForgeKind::Gitlab => Caps {
                topics: true,
                nested_namespaces: true,
                archived: Archived::Archived,
            },
            ForgeKind::Azuredevops => Caps {
                topics: false,
                nested_namespaces: false,
                archived: Archived::Disabled,
            },
        }
    }
}

impl fmt::Display for ForgeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ForgeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        ForgeKind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| {
                format!("unknown forge {s:?}: want one of forgejo, github, gitlab, azuredevops")
            })
    }
}

/// What a forge can express.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Caps {
    /// Repositories carry topics. Azure DevOps has none.
    pub topics: bool,
    /// Namespaces nest below the organisation (GitLab subgroups).
    pub nested_namespaces: bool,
    /// What "archived" is on this forge.
    pub archived: Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Archived {
    /// A read-only repository that still answers reads.
    Archived,
    /// Azure DevOps' `isDisabled`: the repository refuses reads too, so its HEAD is unknown.
    Disabled,
}
