//! Email addresses

#[cfg(feature = "serde")]
mod serde;

mod envelope;
mod types;

// pub: envelope and address types are the public message-routing surface.
pub use self::{
    envelope::Envelope,
    types::{Address, AddressError},
};
