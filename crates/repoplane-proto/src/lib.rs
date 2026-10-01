//! The contract every repoplane binary shares: identifiers, the public API's bodies, and the
//! values that cross a Temporal activity. No I/O lives here.

pub mod api;
pub mod forge;
pub mod id;
pub mod inventory;
pub mod secret;

pub use forge::{Caps, ForgeKind};
