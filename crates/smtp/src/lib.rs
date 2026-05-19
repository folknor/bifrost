//! Bifrost SMTP is an email library that allows creating and sending messages. It provides:
//!
//! * An easy to use email builder
//! * Pluggable email transports
//! * Unicode support
//! * Secure defaults
//! * Async support
//!
//! Bifrost SMTP requires the workspace Rust version or newer.
//!
//! ## Features
//!
//! This section lists each crate feature and briefly explains it.
//! More info about each module can be found in the corresponding module page.
//!
//! Features marked `(default)` are enabled by default.
//!
//! ### Typed message builder
//!
//! _Strongly typed [`message`] builder_
//!
//! * **builder** (default): Enable the [`Message`] builder
//! * **hostname** (default): Try to use the actual system hostname in the `Message-ID` header
//!
//! ### SMTP transport
//!
//! _Send emails using [`SMTP`]_
//!
//! * **smtp-transport** (default): Enable the SMTP transport
//! * **pool** (default): Connection pool for SMTP transport
//! * **hostname** (default): Try to use the actual system hostname for the SMTP `CLIENTID`
//!
//! #### SMTP over TLS via the native-tls crate
//!
//! _Secure SMTP connections using TLS from the `native-tls` crate_
//!
//! Uses schannel on Windows, Security-Framework on macOS, and OpenSSL
//! on all other platforms.
//!
//! * **native-tls** (default): TLS support for the synchronous version of the API
//! * **tokio1-native-tls**: TLS support for the `tokio1` async version of the API
//!
//! NOTE: native-tls for SMTP is only supported with `tokio1`; async-std SMTP
//! remains plaintext-only.
//!
//! ##### Building Bifrost SMTP with OpenSSL
//!
//! When building Bifrost SMTP with native-tls on a system that makes
//! use of OpenSSL, the following packages will need to be installed
//! in order for the build and the compiled program to run properly.
//!
//! | Distro       | Build-time packages        | Runtime packages             |
//! | ------------ | -------------------------- | ---------------------------- |
//! | Debian       | `pkg-config`, `libssl-dev` | `libssl3`, `ca-certificates` |
//! | Alpine Linux | `pkgconf`, `openssl-dev`   | `libssl3`, `ca-certificates` |
//!
//! ### Sendmail transport
//!
//! _Send emails using the [`sendmail`] command_
//!
//! * **sendmail-transport**: Enable the `sendmail` transport
//!
//! ### File transport
//!
//! _Save emails as an `.eml` [`file`]_
//!
//! * **file-transport**: Enable the file transport (saves emails into an `.eml` file)
//! * **file-transport-envelope**: Allow writing the envelope into a JSON file (additionally saves envelopes into a `.json` file)
//!
//! ### Async execution runtimes
//!
//! _Use [tokio] or [async-std] as an async execution runtime for sending emails_
//!
//! The correct runtime version must be chosen in order for Bifrost SMTP to work correctly.
//! For example, when sending emails from a Tokio 1.x context, the Tokio 1.x executor
//! ([`Tokio1Executor`]) must be used. Using a different version (for example Tokio 0.2.x),
//! or async-std, would result in a runtime panic.
//!
//! * **tokio1**: Allow to asynchronously send emails using [Tokio 1.x]
//! * **async-std1**: Allow to asynchronously send emails using [async-std 1.x]
//!
//! NOTE: native-tls for SMTP is only supported with `tokio1`; async-std SMTP
//! remains plaintext-only.
//!
//! ### Misc features
//!
//! _Additional features_
//!
//! * **serde**: Serialization/Deserialization of entities
//! * **tracing**: Logging using the `tracing` crate
//! * **mime03**: Allow creating a [`ContentType`] from an existing [mime 0.3] `Mime` struct
//! * **dkim**: Add support for signing email with DKIM
//! * **web**: WebAssembly support using the `web-time` crate for time operations
//!
//! [`SMTP`]: crate::transport::smtp
//! [`sendmail`]: crate::transport::sendmail
//! [`file`]: crate::transport::file
//! [`ContentType`]: crate::message::header::ContentType
//! [tokio]: https://docs.rs/tokio/1
//! [async-std]: https://docs.rs/async-std/1
//! [Tokio 1.x]: https://docs.rs/tokio/1
//! [async-std 1.x]: https://docs.rs/async-std/1
//! [mime 0.3]: https://docs.rs/mime/0.3
//! [DKIM]: https://datatracker.ietf.org/doc/html/rfc6376

#![doc(html_root_url = "https://docs.rs/bifrost-smtp/0.1.0")]
#![forbid(unsafe_code)]
#![deny(
    unreachable_pub,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unstable_features,
    unused_import_braces,
    rust_2018_idioms,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::clone_on_ref_ptr,
    clippy::verbose_file_reads,
    clippy::unnecessary_self_imports,
    clippy::implicit_clone,
    clippy::mem_forget,
    clippy::cast_lossless,
    clippy::inefficient_to_string,
    clippy::inline_always,
    clippy::linkedlist,
    clippy::macro_use_imports,
    clippy::manual_assert,
    clippy::unnecessary_join,
    clippy::wildcard_imports,
    clippy::str_to_string,
    clippy::empty_structs_with_brackets,
    clippy::zero_sized_map_values,
    clippy::manual_let_else,
    clippy::semicolon_if_nothing_returned,
    clippy::unnecessary_wraps,
    clippy::doc_markdown,
    clippy::explicit_iter_loop,
    clippy::redundant_closure_for_method_calls,
    // Rust 1.86: clippy::unnecessary_semicolon,
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(bifrost_smtp_ignore_tls_mismatch))]
mod compiletime_checks {
    #[cfg(all(
        feature = "tokio1",
        feature = "native-tls",
        not(feature = "tokio1-native-tls")
    ))]
    compile_error!("Bifrost SMTP is being built with the `tokio1` and the `native-tls` features, but the `tokio1-native-tls` feature hasn't been turned on.
    Make sure to apply the same to any of your crate dependencies that use the `bifrost_smtp` crate.");
}

pub mod address;
#[cfg(any(feature = "smtp-transport", feature = "dkim"))]
mod base64;
pub mod error;
#[cfg(any(feature = "tokio1", feature = "async-std1"))]
mod executor;
#[cfg(feature = "builder")]
#[cfg_attr(docsrs, doc(cfg(feature = "builder")))]
pub mod message;
mod time;
pub mod transport;

use std::error::Error as StdError;

#[cfg(feature = "async-std1")]
pub use self::executor::AsyncStd1Executor;
#[cfg(any(feature = "tokio1", feature = "async-std1"))]
pub use self::executor::Executor;
#[cfg(feature = "tokio1")]
pub use self::executor::Tokio1Executor;
#[cfg(any(feature = "tokio1", feature = "async-std1"))]
#[doc(inline)]
pub use self::transport::{AsyncTransport, BoxedAsyncTransport};
pub use crate::address::Address;
#[cfg(feature = "builder")]
#[doc(inline)]
pub use crate::message::Message;
#[cfg(all(
    feature = "file-transport",
    any(feature = "tokio1", feature = "async-std1")
))]
#[doc(inline)]
pub use crate::transport::file::AsyncFileTransport;
#[cfg(feature = "file-transport")]
#[doc(inline)]
pub use crate::transport::file::FileTransport;
#[cfg(all(
    feature = "sendmail-transport",
    any(feature = "tokio1", feature = "async-std1")
))]
#[doc(inline)]
pub use crate::transport::sendmail::AsyncSendmailTransport;
#[cfg(feature = "sendmail-transport")]
#[doc(inline)]
pub use crate::transport::sendmail::SendmailTransport;
#[cfg(all(
    feature = "smtp-transport",
    any(feature = "tokio1", feature = "async-std1")
))]
pub use crate::transport::smtp::AsyncSmtpTransport;
#[cfg(feature = "smtp-transport")]
pub use crate::transport::smtp::SmtpTransport;
#[doc(inline)]
pub use crate::transport::{BoxedTransport, Transport};
use crate::{address::Envelope, error::Error};

#[allow(dead_code)]
pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;

#[cfg(test)]
#[cfg(feature = "builder")]
mod test {
    use super::*;
    use crate::message::{Mailbox, Mailboxes, header, header::Headers};

    #[test]
    fn envelope_from_headers() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let to = Mailboxes::new().with("amousset@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From(from));
        headers.set(header::To(to));

        assert_eq!(
            Envelope::try_from(&headers).unwrap(),
            Envelope::new(
                Some(Address::new("kayo", "example.com").unwrap()),
                vec![Address::new("amousset", "example.com").unwrap()]
            )
            .unwrap()
        );
    }

    #[test]
    fn envelope_from_headers_sender() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let sender = Mailbox::new(None, "kayo2@example.com".parse().unwrap());
        let to = Mailboxes::new().with("amousset@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From::from(from));
        headers.set(header::Sender::from(sender));
        headers.set(header::To::from(to));

        assert_eq!(
            Envelope::try_from(&headers).unwrap(),
            Envelope::new(
                Some(Address::new("kayo2", "example.com").unwrap()),
                vec![Address::new("amousset", "example.com").unwrap()]
            )
            .unwrap()
        );
    }

    #[test]
    fn envelope_from_headers_no_to() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let sender = Mailbox::new(None, "kayo2@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From::from(from));
        headers.set(header::Sender::from(sender));

        assert!(Envelope::try_from(&headers).is_err(),);
    }
}
