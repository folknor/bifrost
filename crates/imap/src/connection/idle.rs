//! IDLE command implementation (RFC 2177, RFC 5465).
//!
//! IDLE is implemented on top of the driver task's typed event queue.
//! The driver enters IDLE-reading mode where it reads wire responses and
//! publishes them via [`DriverEventSink`](super::driver::event_sink::DriverEventSink).
//! The handle-side `idle()` loops over events from
//! [`next_event`](super::ImapConnection::next_event), filtering
//! transparent protocol events (capability changes, server keepalive
//! responses), and returns the first meaningful event as an
//! [`IdleEvent`](super::IdleEvent). It then signals DONE to end the
//! IDLE session.
//!
//! # RFC references
//! - RFC 2177 Sections 2-4 (IDLE command, continuation, DONE)
//! - RFC 9051 Section 6.3.13 (IDLE in `IMAP4rev2`)
//! - RFC 5465 Sections 3-5.8 (NOTIFY event delivery during IDLE,
//!   NOTIFICATIONOVERFLOW, NOTIFY NONE)

use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use super::driver::{DriverCommand, IdleTermination};
use super::typed_event::TypedEvent;
use super::{IdleEvent, ImapConnection};
use crate::error::Error;
use crate::types::response::{UntaggedResponse, UntaggedStatus};

impl ImapConnection {
    /// Enter IDLE mode and wait for the first server event (RFC 2177).
    ///
    /// Sends the IDLE command to the driver task, then waits for
    /// the first event, a timeout, or cancellation. On any of these,
    /// sends DONE to end the IDLE session and returns the result.
    ///
    /// # Cancellation safety
    ///
    /// Dropping the future returned by this method is safe  -  the driver
    /// task owns the stream and will eventually detect that the handle
    /// is gone (channel close). However, if the IDLE command was already
    /// sent on the wire, the connection may be left in an inconsistent
    /// state until the driver task exits. Prefer using the
    /// `cancel` token instead of dropping the future.
    ///
    /// # Filtered events
    ///
    /// Some server responses are handled transparently and do not
    /// interrupt the IDLE wait:
    ///
    /// - **Capability changes** (`* OK [CAPABILITY ...]`)  -  already
    ///   applied by [`ProtocolState::apply_side_effects`]; observe via
    ///   `state_rx` (RFC 3501 Section 7.2.1).
    /// - **Informational keepalives** (`* OK text` with no response
    ///   code)  -  purely informational per RFC 3501 Section 7.1; servers
    ///   like Dovecot send these periodically to keep the TCP connection
    ///   alive.
    ///
    /// # Arguments
    ///
    /// * `timeout`  -  maximum time to wait for an event.
    /// * `cancel`  -  cancellation token; firing this exits IDLE early.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DriverGone`] if the driver task has exited,
    /// or any I/O or protocol error from the IDLE session.
    pub async fn idle(
        &self,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<IdleEvent, Error> {
        // Submit IDLE to the driver task.
        let (done_tx, done_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();
        let dcmd = DriverCommand::Idle { done_rx, result_tx };
        if self.cmd_tx.send(dcmd).await.is_err() {
            return Err(self.observe_driver_panic().await);
        }

        // The driver is now in IDLE mode: it has sent "tag IDLE\r\n",
        // received the `+` continuation, and is reading events from
        // the wire. Events flow through the event_sink into our
        // events_rx channel.
        //
        // We select on three signals:
        // 1. cancel  -  user requested cancellation
        // 2. next_event  -  an event arrived from the server
        //    (includes timeout  -  next_event returns None on timeout)
        // 3. result_rx  -  the driver exited IDLE on its own
        //    (server-terminated or error)

        let deadline = tokio::time::Instant::now() + timeout;
        let mut result_rx = result_rx;

        // Loop until we get a meaningful event, timeout, or
        // cancellation. Some events (e.g., CapabilityChange) are
        // handled transparently by the state machine and should not
        // interrupt IDLE  -  typed_event_to_idle_event returns None
        // for those, and we continue waiting.
        let idle_event: IdleEvent = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break IdleEvent::Timeout;
            }

            tokio::select! {
                biased;
                // P1: Cancellation always wins.
                () = cancel.cancelled() => break IdleEvent::Cancelled,
                // P2: Event from the server (or timeout).
                maybe_ev = self.next_event(remaining) => {
                    match maybe_ev {
                        Ok(Some(ev)) => {
                            if let Some(e) = typed_event_to_idle_event(ev) {
                                break e;
                            }
                        }
                        Ok(None) => break IdleEvent::Timeout,
                        Err(Error::DriverGone) => {
                            // Driver exited  -  don't try to send DONE.
                            return Err(Error::DriverGone);
                        }
                        Err(e) => return Err(e),
                    }
                }
                // P3: Driver exited IDLE on its own (server-terminated
                // or error). Lower priority than events so that events
                // emitted before the tagged OK are delivered first.
                idle_result = &mut result_rx => {
                    return match idle_result {
                        Ok(Ok(IdleTermination::ServerTerminated)) => {
                            Ok(IdleEvent::ServerTerminated)
                        }
                        Ok(Ok(IdleTermination::ClientDone)) => {
                            // Shouldn't happen  -  we haven't sent DONE.
                            // Treat as server-terminated.
                            Ok(IdleEvent::ServerTerminated)
                        }
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(self.observe_driver_panic().await),
                    };
                }
            }
        };

        // Send DONE to the driver and wait for it to finish IDLE.
        let _ = done_tx.send(());
        trace!("idle handle: sent DONE signal");

        // Wait for the driver to send DONE on the wire, drain
        // remaining responses, and confirm the tagged OK.
        match result_rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(self.observe_driver_panic().await),
        }

        Ok(idle_event)
    }
}

/// Convert a [`TypedEvent`] into an [`IdleEvent`], if meaningful.
///
/// Maps the internal event representation to the public IDLE-specific
/// event type. Returns `None` for events that are handled transparently
/// by the protocol state machine and should not interrupt IDLE
/// (e.g., capability changes  -  already applied by `apply_side_effects`,
/// observable via `state_rx`).
///
/// Handles all `TypedEvent` variants, extracting IDLE-relevant data
/// from `Extension` variants where needed (e.g., `MailboxStatus`,
/// `Metadata`, `Search`/`Esearch`).
fn typed_event_to_idle_event(ev: TypedEvent) -> Option<IdleEvent> {
    match ev {
        TypedEvent::Exists(n) => Some(IdleEvent::Exists(n)),
        TypedEvent::Recent(n) => Some(IdleEvent::Recent(n)),
        TypedEvent::Expunge(n) => Some(IdleEvent::Expunge(n)),
        TypedEvent::FetchUpdate(f) => Some(IdleEvent::Fetch(f)),
        TypedEvent::Alert(text) => Some(IdleEvent::Alert(text)),
        TypedEvent::MailboxEvent(info) => Some(IdleEvent::MailboxEvent(info)),
        TypedEvent::Vanished { earlier, uids } => Some(IdleEvent::Vanished { earlier, uids }),
        TypedEvent::NotificationOverflow { code, text } => Some(IdleEvent::NotificationOverflow {
            code_text: code,
            resp_text: text,
        }),
        TypedEvent::CapabilityChange(_) => {
            // Capability changes during IDLE are handled transparently
            // by apply_side_effects  -  the protocol state is already
            // updated and the caller can observe it via state_rx.
            // Surfacing this as an IdleEvent would prematurely end
            // the IDLE wait (RFC 2177 Section 3  -  the client should
            // remain in IDLE until a meaningful mailbox event, timeout,
            // or cancellation occurs).
            trace!("idle: suppressing CapabilityChange (handled by state machine)");
            None
        }
        TypedEvent::Bye { code, text } => Some(IdleEvent::Bye { code, text }),
        TypedEvent::QueueOverflow {
            dropped_count,
            since: _,
        } => Some(IdleEvent::Alert(format!(
            "event queue overflow: {dropped_count} events dropped"
        ))),
        TypedEvent::MetadataChange {} | TypedEvent::ServerMetadataChange {} => {
            // Metadata changes during IDLE  -  no detailed info in
            // the TypedEvent variant. Map to a generic MetadataChange.
            // The caller can re-query metadata if needed.
            Some(IdleEvent::ExtensionEvent(
                "METADATA change during IDLE".into(),
            ))
        }
        TypedEvent::Extension(resp) => untagged_to_idle_event(*resp),
    }
}

/// Convert a raw [`UntaggedResponse`] (wrapped in `TypedEvent::Extension`)
/// into an [`IdleEvent`], if meaningful.
///
/// Returns `None` for purely informational `* OK` keepalives (no response
/// code)  -  these are server-generated heartbeats that should not interrupt
/// IDLE (RFC 3501 Section 7.1).
///
/// Handles response types that `TypedEvent::From<UntaggedResponse>`
/// routes to `Extension`  -  notably `MailboxStatus`, `Metadata`,
/// `Search`, and `Esearch`.
fn untagged_to_idle_event(resp: UntaggedResponse) -> Option<IdleEvent> {
    match resp {
        UntaggedResponse::MailboxStatus { mailbox, items } => {
            Some(IdleEvent::MailboxStatus { mailbox, items })
        }
        UntaggedResponse::Metadata { mailbox, entries } => {
            Some(IdleEvent::MetadataChange { mailbox, entries })
        }
        UntaggedResponse::Search { uids, mod_seq: _ } => {
            // Legacy `* SEARCH` during IDLE  -  wrap in
            // EsearchResponse for the SearchUpdate variant.
            Some(IdleEvent::SearchUpdate(Box::new(
                crate::types::EsearchResponse {
                    tag: None,
                    uid: false,
                    all: uids
                        .into_iter()
                        .map(crate::types::UidRange::single)
                        .collect(),
                    min: None,
                    max: None,
                    count: None,
                    mod_seq: None,
                },
            )))
        }
        UntaggedResponse::Esearch(e) => Some(IdleEvent::SearchUpdate(Box::new(e))),
        UntaggedResponse::Status {
            status,
            code: Some(code),
            text,
        } => Some(IdleEvent::StatusUpdate { status, code, text }),
        // RFC 3501 Section 7.1: `* OK text` with no response code is
        // purely informational. Servers like Dovecot send these as
        // periodic keepalives (e.g., `* OK Still here`) to prevent
        // TCP timeouts. They carry no actionable data and should not
        // interrupt the IDLE wait.
        UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: None,
            text,
        } => {
            trace!("idle: suppressing informational OK keepalive: {text:?}");
            None
        }
        // Everything else: wrap as extension event.
        other => Some(IdleEvent::ExtensionEvent(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "idle_tests.rs"]
mod tests;
