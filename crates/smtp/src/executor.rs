use std::fmt::Debug;
#[cfg(feature = "file-transport")]
use std::io::Result as IoResult;
#[cfg(any(
    feature = "file-transport",
    all(feature = "smtp-transport", feature = "tokio")
))]
use std::path::Path;
#[cfg(feature = "smtp-transport")]
use std::time::Duration;

#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
use crate::transport::smtp::AsyncSmtpConnection;
#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
use crate::transport::smtp::Error;
#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
use crate::transport::smtp::Protocol;
#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
use crate::transport::smtp::Tls;
#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
use crate::transport::smtp::extension::ClientId;

/// Async executor abstraction trait
///
/// Used by [`AsyncSmtpTransport`], [`AsyncSendmailTransport`] and
/// [`AsyncFileTransport`] so tests and transports can share one Tokio-backed
/// abstraction.
///
/// [`AsyncSmtpTransport`]: crate::AsyncSmtpTransport
/// [`AsyncSendmailTransport`]: crate::AsyncSendmailTransport
/// [`AsyncFileTransport`]: crate::AsyncFileTransport
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub trait Executor: Debug + Send + Sync + 'static + private::Sealed {
    #[cfg(feature = "smtp-transport")]
    #[allow(private_bounds)]
    type Handle: SpawnHandle;
    #[cfg(feature = "smtp-transport")]
    type Sleep: Future<Output = ()> + Send + 'static;

    #[doc(hidden)]
    #[cfg(feature = "smtp-transport")]
    fn spawn<F>(fut: F) -> Self::Handle
    where
        F: Future<Output = ()> + Send + 'static,
        F::Output: Send + 'static;

    #[doc(hidden)]
    #[cfg(feature = "smtp-transport")]
    fn sleep(duration: Duration) -> Self::Sleep;

    #[doc(hidden)]
    #[cfg(feature = "file-transport-envelope")]
    fn fs_read(path: &Path) -> impl Future<Output = IoResult<Vec<u8>>> + Send + '_;

    #[doc(hidden)]
    #[cfg(feature = "file-transport")]
    fn fs_write<'a>(
        path: &'a Path,
        contents: &'a [u8],
    ) -> impl Future<Output = IoResult<()>> + Send + 'a;
}

#[doc(hidden)]
#[cfg(feature = "smtp-transport")]
pub(crate) trait SpawnHandle: Debug + Send + Sync + 'static + private::Sealed {
    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_;
}

#[cfg(feature = "smtp-transport")]
pub(crate) trait SmtpExecutor: Executor {
    // Keep SMTP dialing out of the public `Executor` trait. Public async
    // transport constructors intentionally use this private sealed bound, so
    // those call sites carry `#[allow(private_bounds)]`.
    fn connect<'a>(
        hostname: &'a str,
        port: u16,
        unix_socket: Option<&'a Path>,
        timeout: Option<Duration>,
        hello_name: &'a ClientId,
        tls: &'a Tls,
        protocol: Protocol,
    ) -> impl Future<Output = Result<AsyncSmtpConnection, Error>> + Send + 'a;
}

/// Async [`Executor`] using Tokio.
///
/// Used by [`AsyncSmtpTransport`], [`AsyncSendmailTransport`] and [`AsyncFileTransport`]
/// for async runtime services.
///
/// [`AsyncSmtpTransport`]: crate::AsyncSmtpTransport
/// [`AsyncSendmailTransport`]: crate::AsyncSendmailTransport
/// [`AsyncFileTransport`]: crate::AsyncFileTransport
#[allow(missing_copy_implementations)]
#[non_exhaustive]
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[derive(Debug)]
pub struct TokioExecutor;

#[cfg(feature = "tokio")]
impl Executor for TokioExecutor {
    #[cfg(feature = "smtp-transport")]
    type Handle = tokio::task::JoinHandle<()>;
    #[cfg(feature = "smtp-transport")]
    type Sleep = tokio::time::Sleep;

    #[cfg(feature = "smtp-transport")]
    fn spawn<F>(fut: F) -> Self::Handle
    where
        F: Future<Output = ()> + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(fut)
    }

    #[cfg(feature = "smtp-transport")]
    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    #[cfg(feature = "file-transport-envelope")]
    fn fs_read(path: &Path) -> impl Future<Output = IoResult<Vec<u8>>> + Send + '_ {
        tokio::fs::read(path)
    }

    #[cfg(feature = "file-transport")]
    fn fs_write<'a>(
        path: &'a Path,
        contents: &'a [u8],
    ) -> impl Future<Output = IoResult<()>> + Send + 'a {
        tokio::fs::write(path, contents)
    }
}

#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
impl SmtpExecutor for TokioExecutor {
    async fn connect(
        hostname: &str,
        port: u16,
        unix_socket: Option<&Path>,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls: &Tls,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        if let Some(path) = unix_socket {
            #[cfg(unix)]
            {
                if !matches!(tls, Tls::None) {
                    return Err(crate::transport::smtp::error::client(
                        "TLS is not supported over Unix-domain LMTP sockets",
                    ));
                }
                return AsyncSmtpConnection::connect_unix_with_protocol(
                    path, timeout, hello_name, protocol,
                )
                .await;
            }
            #[cfg(not(unix))]
            {
                // Keep the binding used when Unix socket support is cfg-gated out.
                let _ = path;
                return Err(crate::transport::smtp::error::client(
                    "Unix-domain LMTP sockets are only supported on Unix platforms",
                ));
            }
        }

        #[allow(clippy::match_single_binding)]
        let tls_parameters = match tls {
            #[cfg(feature = "tokio")]
            Tls::Wrapper(tls_parameters) => Some(tls_parameters.clone()),
            _ => None,
        };
        #[allow(unused_mut)]
        let mut conn = AsyncSmtpConnection::connect_with_protocol(
            (hostname, port),
            timeout,
            hello_name,
            tls_parameters,
            None,
            protocol,
        )
        .await?;

        #[cfg(feature = "tokio")]
        match tls {
            Tls::Opportunistic(tls_parameters) if conn.can_starttls() => {
                conn.starttls(tls_parameters.clone(), hello_name).await?;
            }
            Tls::Required(tls_parameters) => {
                conn.starttls(tls_parameters.clone(), hello_name).await?;
            }
            _ => (),
        }

        Ok(conn)
    }
}

#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
impl SpawnHandle for tokio::task::JoinHandle<()> {
    async fn shutdown(&self) {
        self.abort();
    }
}

mod private {
    pub trait Sealed {}

    #[cfg(feature = "tokio")]
    impl Sealed for super::TokioExecutor {}

    #[cfg(all(feature = "smtp-transport", feature = "tokio"))]
    impl Sealed for tokio::task::JoinHandle<()> {}
}
