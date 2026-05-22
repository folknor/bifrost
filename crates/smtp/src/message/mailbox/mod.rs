mod parsers;
#[cfg(feature = "serde")]
mod serde;
mod types;

// pub: users construct typed RFC 5322 mailbox values.
pub use self::types::*;
