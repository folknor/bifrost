//! The stub transport logs message envelopes as well as contents. It can be useful for testing
//! purposes.
//!
//! # Async stub transport
//!
//! The stub transport logs message envelopes as well as contents. It can be useful for testing
//! purposes.
//!
//! # Examples
//!
//! ```rust,ignore
//! # //! # {
//! use bifrost_smtp::{
//!     AsyncTransport, Message, message::header::ContentType,
//!     transport::stub::AsyncStubTransport,
//! };
//!
//! # use std::error::Error;
//! # async fn try_main() -> Result<(), Box<dyn Error>> {
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! let sender = AsyncStubTransport::new_ok();
//! sender.send(&email).await?;
//! assert_eq!(
//!     sender.messages().await,
//!     vec![(
//!         email.envelope().clone(),
//!         String::from_utf8(email.formatted()).unwrap()
//!     )],
//! );
//! # Ok(())
//! # }
//! ```

use std::{
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
};

use crate::AsyncTransport;
use crate::address::Envelope;

/// An error returned by the stub transport
#[non_exhaustive]
#[derive(Debug, Copy, Clone)]
// pub: users can assert the stub transport's configured failure.
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stub error")
    }
}

impl StdError for Error {}

/// This transport logs messages and always returns the given response
#[derive(Debug, Clone)]
// pub: users can substitute an async logging transport in tests.
pub struct AsyncStubTransport {
    response: Result<(), Error>,
    message_log: Arc<Mutex<Vec<(Envelope, String)>>>,
}

impl AsyncStubTransport {
    /// Creates a new transport that always returns the given Result
    pub fn new(response: Result<(), Error>) -> Self {
        Self {
            response,
            message_log: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Creates a new transport that always returns a success response
    pub fn new_ok() -> Self {
        Self {
            response: Ok(()),
            message_log: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Creates a new transport that always returns an error
    pub fn new_error() -> Self {
        Self {
            response: Err(Error),
            message_log: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Return all logged messages sent using [`AsyncTransport::send_raw`]
    pub async fn messages(&self) -> Vec<(Envelope, String)> {
        self.message_log.lock().unwrap().clone()
    }
}

impl AsyncTransport for AsyncStubTransport {
    type Ok = ();
    type Error = Error;

    async fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.message_log
            .lock()
            .unwrap()
            .push((envelope.clone(), String::from_utf8_lossy(email).into()));
        self.response
    }
}
