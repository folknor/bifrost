//! ## Transports for sending emails
//!
//! This module contains `Transport`s for sending emails. A `Transport` implements a high-level API
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
//! * Use the [`SMTP`] transport, with the [`relay`] builder (or one of its async counterparts)
//!   with your server's hostname. They provide modern and secure defaults.
//! * Use the [`credentials`] method of the builder to pass your credentials.
//!
//! These should be enough to safely cover most use cases.
//!
//! ### Available transports
//!
//! The following transports are available:
//!
//! | Module   | Protocol | Sync API              | Async API              | Description                                             |
//! | -------- | -------- | --------------------- | ---------------------- | ------------------------------------------------------- |
//! | [`smtp`] | SMTP     | [`SmtpTransport`]     | [`AsyncSmtpTransport`] | Uses the SMTP protocol to send emails to a relay server |
//! | [`stub`] | Debug    | [`StubTransport`]     | [`AsyncStubTransport`] | Drops the email - Useful for debugging                  |
//!
//! ## Building an email
//!
//! Emails can either be built though [`Message`], which is a typed API for constructing emails
//! (find out more about it by going over the [`message`][crate::message] module),
//! or via external means.
//!
//! [`Message`]s can be sent via [`Transport::send`] or [`AsyncTransport::send`], while messages
//! built without the crate's [`message`][crate::message] APIs can be sent via [`Transport::send_raw`]
//! or [`AsyncTransport::send_raw`].
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
//! # fn main() -> Result<(), Box<dyn Error>> {
//! use bifrost_smtp::{
//!     Message, SmtpTransport, Transport, message::header::ContentType,
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
//! let mailer = SmtpTransport::relay("smtp.gmail.com")?
//!     .password("smtp_username", "smtp_password")
//!     .build();
//!
//! // Send the email
//! match mailer.send(&email) {
//!     Ok(_) => println!("Email sent successfully!"),
//!     Err(e) => panic!("Could not send email: {e:?}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [MTA]: https://en.wikipedia.org/wiki/Message_transfer_agent
//! [`SMTP`]: crate::transport::smtp
//! [`relay`]: crate::SmtpTransport::relay
//! [`starttls_relay`]: crate::SmtpTransport::starttls_relay
//! [`credentials`]: crate::transport::smtp::SmtpTransportBuilder::credentials
//! [`Message`]: crate::Message
//! [`SmtpTransport`]: crate::SmtpTransport
//! [`AsyncSmtpTransport`]: crate::AsyncSmtpTransport
//! [`StubTransport`]: crate::transport::stub::StubTransport
//! [`AsyncStubTransport`]: crate::transport::stub::AsyncStubTransport

use crate::Envelope;
use crate::Message;

// pub: users configure and send through the SMTP/LMTP transport module.
pub mod smtp;
// pub: users can swap in a logging transport for tests and dry runs.
pub mod stub;

/// Blocking Transport method for emails
// pub: concrete transports implement this user-facing send trait.
pub trait Transport {
    /// Response produced by the Transport
    type Ok;
    /// Error produced by the Transport
    type Error;

    /// Sends the email
    fn send(&self, message: &Message) -> Result<Self::Ok, Self::Error> {
        #[cfg(feature = "tracing")]
        tracing::trace!("starting to send an email");

        let raw = message.formatted();
        self.send_raw(message.envelope(), &raw)
    }

    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error>;

    /// Shuts down the transport. Future calls to [`Self::send`] and
    /// [`Self::send_raw`] might fail.
    fn shutdown(&self) {}
}

/// Boxed blocking transport trait object.
// pub: users can erase concrete sync transport choices.
pub type BoxedTransport<Ok, Error> = Box<dyn Transport<Ok = Ok, Error = Error> + Send + Sync>;

impl<T> Transport for Box<T>
where
    T: Transport + ?Sized,
{
    type Ok = T::Ok;
    type Error = T::Error;

    fn send(&self, message: &Message) -> Result<Self::Ok, Self::Error> {
        (**self).send(message)
    }

    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        (**self).send_raw(envelope, email)
    }

    fn shutdown(&self) {
        (**self).shutdown();
    }
}

impl<T> Transport for std::sync::Arc<T>
where
    T: Transport + ?Sized,
{
    type Ok = T::Ok;
    type Error = T::Error;

    fn send(&self, message: &Message) -> Result<Self::Ok, Self::Error> {
        (**self).send(message)
    }

    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        (**self).send_raw(envelope, email)
    }

    fn shutdown(&self) {
        (**self).shutdown();
    }
}

/// Async Transport method for emails
///
/// Implementations must be [`Sync`] so borrowed async methods can return
/// [`Send`] futures.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
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

#[cfg(feature = "tokio")]
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

#[cfg(feature = "tokio")]
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

#[cfg(feature = "tokio")]
trait ErasedAsyncTransport<Ok, Error>: Send + Sync {
    fn send_raw_boxed<'a>(
        &'a self,
        envelope: &'a Envelope,
        email: &'a [u8],
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Ok, Error>> + Send + 'a>>;

    fn shutdown_boxed(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[cfg(feature = "tokio")]
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
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
// pub: users can erase concrete async transport choices.
pub struct BoxedAsyncTransport<Ok, Error> {
    inner: Box<dyn ErasedAsyncTransport<Ok, Error>>,
}

#[cfg(feature = "tokio")]
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

#[cfg(feature = "tokio")]
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
