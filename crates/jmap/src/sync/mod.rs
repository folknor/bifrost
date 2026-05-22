//! `bifrost-types::Account` implementation for `bifrost-jmap`.
//!
//! The implementation is intentionally engine-facing only: this crate
//! depends on `bifrost-types`, not on `bifrost-sync`. JMAP change
//! cursors are encoded as protocol-tagged opaque bytes and every stream
//! emits checkpoints at server state boundaries.

pub mod account;
pub mod blob;
pub mod capabilities;
pub mod changes;
pub mod discover;
pub mod error;
pub mod factory;
pub mod hydrate;
pub mod inventory;
pub mod mutation;
pub mod push;
pub mod state;

pub use account::JmapAccount;
pub use factory::{JmapAccountFactory, JmapAccountFactoryBuilder, JmapCredentials};
pub use push::ReconnectPolicy;
