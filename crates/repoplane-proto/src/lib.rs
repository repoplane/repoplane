//! The contract every repoplane binary shares, and the only crate all three link: identifiers,
//! the public API's bodies, and the worker protocol. No I/O lives here.

pub mod api;
pub mod forge;
pub mod id;
pub mod secret;
pub mod worker;

pub use forge::{Caps, ForgeKind};
