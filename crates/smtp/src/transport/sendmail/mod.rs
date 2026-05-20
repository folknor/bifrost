//! The sendmail transport sends the email using the local `sendmail` command.
//!
//! ## Sync example
//!
//! ```rust
//! # use std::error::Error;
//! #
//! # #[cfg(all(feature = "sendmail-transport", feature = "builder"))]
//! # fn main() -> Result<(), Box<dyn Error>> {
//! use bifrost_smtp::{Message, SendmailTransport, Transport, message::header::ContentType};
//!
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! let sender = SendmailTransport::new();
//! sender.send(&email)?;
//! # Ok(())
//! # }
//!
//! # #[cfg(not(all(feature = "sendmail-transport", feature = "builder")))]
//! # fn main() {}
//! ```
//!
//! ## Async tokio 1.x example
//!
//! ```rust,no_run
//! # use std::error::Error;
//! #
//! # #[cfg(all(feature = "tokio", feature = "sendmail-transport", feature = "builder"))]
//! # async fn run() -> Result<(), Box<dyn Error>> {
//! use bifrost_smtp::{
//!     AsyncSendmailTransport, AsyncTransport, Message, SendmailTransport, TokioExecutor,
//!     message::header::ContentType,
//! };
//!
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! let sender = AsyncSendmailTransport::<TokioExecutor>::new();
//! sender.send(&email).await?;
//! # Ok(())
//! # }
//! ```
//!
#[cfg(feature = "tokio")]
use std::marker::PhantomData;
use std::{
    ffi::OsString,
    io::Write,
    process::{Command, Stdio},
};

pub use self::error::Error;
#[cfg(feature = "tokio")]
use crate::TokioExecutor;
#[cfg(feature = "tokio")]
use crate::{AsyncTransport, Executor};
use crate::{Transport, address::Envelope};

mod error;

const DEFAULT_SENDMAIL: &str = "sendmail";

/// Sends emails using the `sendmail` command
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(docsrs, doc(cfg(feature = "sendmail-transport")))]
pub struct SendmailTransport {
    command: OsString,
}

/// Asynchronously sends emails using the `sendmail` command
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct AsyncSendmailTransport<E: Executor> {
    inner: SendmailTransport,
    marker_: PhantomData<E>,
}

impl SendmailTransport {
    /// Creates a new transport with the `sendmail` command
    ///
    /// Note: This uses the `sendmail` command in the current `PATH`. To use another command,
    /// use [`SendmailTransport::new_with_command`].
    pub fn new() -> SendmailTransport {
        SendmailTransport {
            command: DEFAULT_SENDMAIL.into(),
        }
    }

    /// Creates a new transport to the given sendmail command
    pub fn new_with_command<S: Into<OsString>>(command: S) -> SendmailTransport {
        SendmailTransport {
            command: command.into(),
        }
    }

    fn command(&self, envelope: &Envelope) -> Command {
        let mut c = Command::new(&self.command);
        c.arg("-i");
        if let Some(from) = envelope.from() {
            c.arg("-f").arg(from);
        }
        c.arg("--")
            .args(envelope.to())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }
}

#[cfg(feature = "tokio")]
impl<E> AsyncSendmailTransport<E>
where
    E: Executor,
{
    /// Creates a new transport with the `sendmail` command
    ///
    /// Note: This uses the `sendmail` command in the current `PATH`. To use another command,
    /// use [`AsyncSendmailTransport::new_with_command`].
    pub fn new() -> Self {
        Self {
            inner: SendmailTransport::new(),
            marker_: PhantomData,
        }
    }

    /// Creates a new transport to the given sendmail command
    pub fn new_with_command<S: Into<OsString>>(command: S) -> Self {
        Self {
            inner: SendmailTransport::new_with_command(command),
            marker_: PhantomData,
        }
    }

    fn tokio_command(&self, envelope: &Envelope) -> tokio::process::Command {
        use tokio::process::Command;

        let mut c = Command::new(&self.inner.command);
        c.kill_on_drop(true);
        c.arg("-i");
        if let Some(from) = envelope.from() {
            c.arg("-f").arg(from);
        }
        c.arg("--")
            .args(envelope.to())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }
}

impl Default for SendmailTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tokio")]
impl<E> Default for AsyncSendmailTransport<E>
where
    E: Executor,
{
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for SendmailTransport {
    type Ok = ();
    type Error = Error;

    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        #[cfg(feature = "tracing")]
        tracing::debug!(command = ?self.command, "sending email with");

        // Spawn the sendmail command
        let mut process = self.command(envelope).spawn().map_err(error::client)?;

        process
            .stdin
            .as_mut()
            .unwrap()
            .write_all(email)
            .map_err(error::client)?;
        let output = process.wait_with_output().map_err(error::client)?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8(output.stderr).map_err(error::response)?;
            Err(error::client(stderr))
        }
    }
}

#[cfg(feature = "tokio")]
impl AsyncTransport for AsyncSendmailTransport<TokioExecutor> {
    type Ok = ();
    type Error = Error;

    async fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        use tokio::io::AsyncWriteExt;

        #[cfg(feature = "tracing")]
        tracing::debug!(command = ?self.inner.command, "sending email with");

        let mut command = self.tokio_command(envelope);

        // Spawn the sendmail command
        let mut process = command.spawn().map_err(error::client)?;

        process
            .stdin
            .as_mut()
            .unwrap()
            .write_all(email)
            .await
            .map_err(error::client)?;
        let output = process.wait_with_output().await.map_err(error::client)?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8(output.stderr).map_err(error::response)?;
            Err(error::client(stderr))
        }
    }
}
