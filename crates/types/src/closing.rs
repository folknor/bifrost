//! The closed-account gate: one admission boundary for every `Account` method.
//!
//! [`ClosingAccount`] wraps any provider. Its `Account` impl is generated from
//! the trait declaration by `bifrost_types_macros::closing_account`, driven by
//! the `#[admission(..)]` policy every trait method carries, so the wrapper
//! forwards every method - including the provider's overrides of default
//! bodies - and a new trait method cannot compile without a policy.
//!
//! The contract, stated once on `Account::close`:
//!
//! - **Refuse new work.** A future or stream whose first poll finds the
//!   account closed yields `AccountError::account_closed` (terminal
//!   `Cancelled`) without calling the provider at all: no I/O, no transport
//!   error, no success. A future built before close and first polled after it
//!   is refused. Admission is decided exactly once per future or stream, at
//!   the open-state check inside its first poll, and remembered: a call
//!   admitted while open stays admitted when its first poll was `Pending`.
//! - **Settle admitted work.** An admitted call keeps the provider's result,
//!   including any shutdown error and transmission evidence the provider
//!   reports when close interrupts it. Replacing it with a refusal could hide
//!   a committed write.
//! - **Classification stays live.** The synchronous, pure accessors are
//!   forwarded after close ([`ClosingAccount::EXEMPT_METHODS`]), and
//!   `inventory_resume_stream` keeps the provider's `Some`/`None` answer so a
//!   stored cursor's identity and its agreement with `is_inventory_cursor` do
//!   not change; only the stream inside `Some` is refused at first poll. No
//!   classification answer is provider work: every path from it to the
//!   provider passes through a gated method.
//! - **Close.** `close` shuts admission synchronously when it is CALLED, so a
//!   close future dropped unpolled still closes the account, then calls the
//!   provider's `close` and returns that future. The wrapper certifies only
//!   admission closure; awaiting `close` awaits that provider invocation, and
//!   whether concurrent closes share one teardown is the provider's contract.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;

use crate::Protocol;
use crate::account::{Account, AccountFuture, AccountStream};
use crate::error::{AccountError, AccountOperation};
use crate::events::{InventoryEvent, SyncEvent};

/// An account behind the closed-account gate. See the module docs.
pub struct ClosingAccount<A: ?Sized> {
    pub(crate) inner: Arc<A>,
    pub(crate) admission: Admission,
}

impl<A: ?Sized> ClosingAccount<A> {
    /// Put `inner` behind the gate. Every handle to the result shares one
    /// admission state; `inner`'s other handles bypass it.
    pub fn new(inner: Arc<A>) -> Self {
        Self {
            inner,
            admission: Admission::default(),
        }
    }

    /// Whether `close` has been called on this gate.
    pub fn is_closed(&self) -> bool {
        self.admission.is_closed()
    }
}

impl ClosingAccount<dyn Account> {
    /// Wrap an erased account, returning the erased gate.
    pub fn wrap(inner: Arc<dyn Account>) -> Arc<dyn Account> {
        Arc::new(Self::new(inner))
    }
}

impl<A: ?Sized> std::fmt::Debug for ClosingAccount<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosingAccount")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// Shared open/closed state of one gate.
#[derive(Clone, Debug, Default)]
pub(crate) struct Admission {
    closed: Arc<AtomicBool>,
}

impl Admission {
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Return shapes an exempt method may have: synchronous values that cannot
/// carry a refusal. The marker checks the shape only; whether a method is
/// pure enough to stay live after close is the `#[admission(exempt)]`
/// annotation's claim.
pub(crate) trait Exempt {}

impl Exempt for Protocol {}
impl Exempt for &crate::capabilities::AccountCapabilities {}
impl Exempt for () {}
impl Exempt for bool {}
impl Exempt for Vec<crate::ThrottleKey> {}
impl Exempt for crate::cursor::CursorDescriptor {}
impl Exempt for crate::events::InventoryPartitioning {}
impl Exempt for crate::FlagTargetState {}
impl Exempt for Vec<crate::PushLaneSpec> {}

pub(crate) fn exempt<T: Exempt>(value: T) -> T {
    value
}

/// A return shape whose provider call can be deferred to first poll and
/// replaced by a refusal.
pub(crate) trait Gated: Sized {
    fn gated(
        admission: Admission,
        operation: AccountOperation,
        protocol: Protocol,
        call: impl FnOnce() -> Self + Send + 'static,
    ) -> Self;
}

/// One item type a gated stream ends with when refused.
pub(crate) trait Refusal {
    /// The terminal item carrying `error`. It makes no completeness claim.
    fn refusal(error: AccountError) -> Self;
}

impl<T> Refusal for SyncEvent<T> {
    fn refusal(error: AccountError) -> Self {
        Self::Terminated(error)
    }
}

impl Refusal for InventoryEvent {
    fn refusal(error: AccountError) -> Self {
        Self::Terminated(error)
    }
}

impl Refusal for crate::cursor::ScopeLifecycleEvent {
    fn refusal(error: AccountError) -> Self {
        Self::Terminated(error)
    }
}

impl Refusal for crate::repair::InventoryRepairEvent {
    // The repair contract lets `Terminated` stand without per-request
    // outcomes; the engine defers what is outstanding. The request stream is
    // dropped unpolled rather than drained, so a refused call never drives
    // caller-owned input.
    fn refusal(error: AccountError) -> Self {
        Self::Terminated(error)
    }
}

impl Refusal for crate::PushSessionEvent {
    // A refused session never started, so it has no cleanup to report and
    // ends on its classified failure without `Established` or `Healthy`.
    fn refusal(error: AccountError) -> Self {
        Self::Failed(error)
    }
}

impl<T: Send + 'static> Gated for AccountFuture<Result<T, AccountError>> {
    fn gated(
        admission: Admission,
        operation: AccountOperation,
        protocol: Protocol,
        call: impl FnOnce() -> Self + Send + 'static,
    ) -> Self {
        Box::pin(async move {
            if admission.is_closed() {
                return Err(AccountError::account_closed(operation, protocol));
            }
            call().await
        })
    }
}

impl<E: Refusal + Send + 'static> Gated for AccountStream<E> {
    fn gated(
        admission: Admission,
        operation: AccountOperation,
        protocol: Protocol,
        call: impl FnOnce() -> Self + Send + 'static,
    ) -> Self {
        Box::pin(
            futures::stream::once(async move {
                if admission.is_closed() {
                    refused(operation, protocol)
                } else {
                    call()
                }
            })
            .flatten(),
        )
    }
}

fn refused<E: Refusal + Send + 'static>(
    operation: AccountOperation,
    protocol: Protocol,
) -> AccountStream<E> {
    let error = AccountError::account_closed(operation, protocol);
    Box::pin(futures::stream::once(async move { E::refusal(error) }))
}

pub(crate) fn gate<R: Gated>(
    admission: Admission,
    operation: AccountOperation,
    protocol: Protocol,
    call: impl FnOnce() -> R + Send + 'static,
) -> R {
    R::gated(admission, operation, protocol, call)
}

/// Gate a stream the provider already constructed (inertly, per the
/// `inventory_resume_stream` contract). Construction is not admission: the
/// stream is admitted, or dropped unpolled and refused, at its first poll.
pub(crate) fn gate_constructed_stream<E: Refusal + Send + 'static>(
    admission: Admission,
    operation: AccountOperation,
    protocol: Protocol,
    stream: AccountStream<E>,
) -> AccountStream<E> {
    Box::pin(
        futures::stream::once(async move {
            if admission.is_closed() {
                drop(stream);
                refused(operation, protocol)
            } else {
                stream
            }
        })
        .flatten(),
    )
}

#[cfg(test)]
#[path = "closing_tests.rs"]
mod tests;
