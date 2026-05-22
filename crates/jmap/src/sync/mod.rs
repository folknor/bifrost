//! `bifrost-types::Account` implementation for `bifrost-jmap`.
//!
//! The implementation is intentionally engine-facing only: this crate
//! depends on `bifrost-types`, not on `bifrost-sync`. JMAP change
//! cursors are encoded as protocol-tagged opaque bytes and every stream
//! emits checkpoints at server state boundaries.

mod account;
mod blob;
mod capabilities;
mod changes;
mod discover;
mod error;
mod factory;
mod hydrate;
mod inventory;
mod mutation;
mod push;
mod state;

// pub: engine registration surface consumed by bifrost-sync users.
pub use factory::{JmapAccountFactory, JmapAccountFactoryBuilder, JmapCredentials};
// pub: account-open configuration for JMAP WebSocket reconnect behavior.
pub use push::ReconnectPolicy;
