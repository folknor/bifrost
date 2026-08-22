//! ## Transports for sending emails
//!
//! This module contains async transports for sending emails. An `AsyncTransport` implements a high-level API
//! for sending emails. It automatically manages the underlying resources and doesn't require any
//! specific knowledge of email protocols in order to be used.
//!
//! ### Getting started
//!
//! Sending emails from your programs requires using an email relay, as client libraries are not
//! designed to handle email delivery by themselves. Depending on your infrastructure, your relay
//! could be:
//!
//! * a service from your Cloud or hosting provider
//! * an email server ([MTA] for Mail Transfer Agent, like Postfix or Exchange), running either
//!   locally on your servers or accessible over the network
//! * a dedicated external service, like Mailchimp, Mailgun, etc.
//!
//! In most cases, the best option is to:
//!
//! * Use the [`SMTP`] transport with the [`relay`] builder
//!   with your server's hostname. They provide modern and secure defaults.
//! * Use the [`credentials`] method of the builder to pass your credentials.
//!
//! These should be enough to safely cover most use cases.
//!
//! ### Available transports
//!
//! The following transports are available:
//!
//! | Module   | Protocol | Async API              | Description                                             |
//! | -------- | -------- | ---------------------- | ------------------------------------------------------- |
//! | [`smtp`] | SMTP     | [`AsyncSmtpTransport`] | Uses the SMTP protocol to send emails to a relay server |
//! | [`stub`] | Debug    | [`AsyncStubTransport`] | Logs email for debugging and tests                      |
//!
//! ## Building an email
//!
//! Emails can either be built though [`Message`], which is a typed API for constructing emails
//! (find out more about it by going over the [`message`][crate::message] module),
//! or via external means.
//!
//! [`Message`]s can be sent via [`AsyncTransport::send`], while messages
//! built without the crate's [`message`][crate::message] APIs can be sent via
//! [`AsyncTransport::send_raw`].
//!
//! ## Brief example
//!
//! This example shows how to build an email and send it via an SMTP relay server.
//! It is in no way a complete example, but it shows how to get started with Bifrost SMTP.
//! More examples can be found by looking at the specific modules, linked in the _Module_ column
//! of the [table above](#transports-for-sending-emails).
//!
//! ```rust,no_run
//! # use std::error::Error;
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn Error>> {
//! use bifrost_smtp::{
//!     AsyncSmtpTransport, AsyncTransport, Message, TokioExecutor,
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
//! // Open a remote connection to the SMTP relay server
//! let mailer = AsyncSmtpTransport::<TokioExecutor>::relay("smtp.gmail.com")?
//!     .password("smtp_username", "smtp_password")
//!     .build();
//!
//! // Send the email
//! match mailer.send(&email).await {
//!     Ok(_) => println!("Email sent successfully!"),
//!     Err(e) => panic!("Could not send email: {e:?}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [MTA]: https://en.wikipedia.org/wiki/Message_transfer_agent
//! [`SMTP`]: crate::transport::smtp
//! [`relay`]: crate::AsyncSmtpTransport::relay
//! [`credentials`]: crate::transport::smtp::AsyncSmtpTransportBuilder::credentials
//! [`Message`]: crate::Message
//! [`AsyncSmtpTransport`]: crate::AsyncSmtpTransport
//! [`AsyncStubTransport`]: crate::transport::stub::AsyncStubTransport

use crate::Envelope;
use crate::Message;

// pub: users configure and send through the SMTP/LMTP transport module.
pub mod smtp;
// pub: users can swap in a logging transport for tests and dry runs.
pub mod stub;

/// Async Transport method for emails
///
/// Implementations must be [`Sync`] so borrowed async methods can return
/// [`Send`] futures.
// pub: concrete async transports implement this user-facing send trait.
pub trait AsyncTransport: Sync {
    /// Response produced by the Transport
    type Ok;
    /// Error produced by the Transport
    type Error;

    /// Sends the email
    fn send<'a>(
        &'a self,
        message: &'a Message,
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        async move {
            #[cfg(feature = "tracing")]
            tracing::trace!("starting to send an email");

            let raw = message.formatted();
            self.send_raw(message.envelope(), &raw).await
        }
    }

    fn send_raw<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a;

    /// Shuts down the transport. Future calls to [`Self::send`] and
    /// [`Self::send_raw`] might fail.
    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
}

impl<T> AsyncTransport for Box<T>
where
    T: AsyncTransport + ?Sized,
{
    type Ok = T::Ok;
    type Error = T::Error;

    fn send<'a>(
        &'a self,
        message: &'a Message,
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        (**self).send(message)
    }

    fn send_raw<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        (**self).send_raw(envelope, email)
    }

    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).shutdown()
    }
}

impl<T> AsyncTransport for std::sync::Arc<T>
where
    T: AsyncTransport + Send + Sync + ?Sized,
{
    type Ok = T::Ok;
    type Error = T::Error;

    fn send<'a>(
        &'a self,
        message: &'a Message,
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        (**self).send(message)
    }

    fn send_raw<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        (**self).send_raw(envelope, email)
    }

    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_ {
        (**self).shutdown()
    }
}

trait ErasedAsyncTransport<Ok, Error>: Send + Sync {
    fn send_raw_boxed<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Ok, Error>> + Send + 'a>>;

    fn shutdown_boxed(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl<Ok, Error, T> ErasedAsyncTransport<Ok, Error> for T
where
    T: AsyncTransport<Ok = Ok, Error = Error> + Send + Sync,
{
    fn send_raw_boxed<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Ok, Error>> + Send + 'a>> {
        Box::pin(self.send_raw(envelope, email))
    }

    fn shutdown_boxed(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.shutdown())
    }
}

/// Boxed async transport.
///
/// `AsyncTransport` uses native `impl Future` return types and is not object
/// safe. This adapter provides a stable boxed async transport value without
/// reintroducing `async-trait`. The wrapped transport must be `Send + Sync +
/// 'static` because the heap-erased transport can outlive the construction
/// frame and may be shared between runtime tasks.
// pub: users can erase concrete async transport choices.
pub struct BoxedAsyncTransport<Ok, Error> {
    inner: Box<dyn ErasedAsyncTransport<Ok, Error>>,
}

impl<Ok, Error> BoxedAsyncTransport<Ok, Error> {
    /// Boxes an async transport.
    pub fn new<T>(transport: T) -> Self
    where
        T: AsyncTransport<Ok = Ok, Error = Error> + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(transport),
        }
    }
}

impl<Ok, Error> AsyncTransport for BoxedAsyncTransport<Ok, Error> {
    type Ok = Ok;
    type Error = Error;

    fn send_raw<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send + 'a {
        self.inner.send_raw_boxed(envelope, email)
    }

    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_ {
        self.inner.shutdown_boxed()
    }
}
