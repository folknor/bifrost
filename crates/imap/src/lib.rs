//! IMAP implementation of the shared bifrost `Account` surface.
//!
//! Consumers construct an [`ImapAccountFactory`] from [`ImapAccountConfig`],
//! [`ImapConfig`], [`Credentials`], and [`AuthPolicy`], then register it as a
//! `bifrost_types::AccountFactory`. The raw IMAP connection, parser, and wire
//! types are crate-internal implementation detail.

// The Account impl drives a subset of the IMAP4rev2 + extensions surface
// implemented under `connection/` and `types/`. The remaining commands,
// response shapes, and pipeline helpers stay built so the protocol layer
// is complete; a future trimming pass can decide what to remove vs wire
// up. The annotation is at the crate root because the dead surface is
// spread across most of the connection module tree.
#![allow(dead_code)]

// pub: consumers register the IMAP AccountFactory through this module.
pub mod account;

mod codec;
mod connection;
mod error;
mod types;

pub use account::{ImapAccountConfig, ImapAccountFactory, ManageSieveConfig};
pub use connection::ImapConfig;
pub use types::{AuthPolicy, Credentials};

pub(crate) use connection::{ImapConnection, typed_event::TypedEvent};
pub(crate) use error::Error;

/// Result type alias for IMAP operations.
pub(crate) type Result<T> = std::result::Result<T, Error>;
