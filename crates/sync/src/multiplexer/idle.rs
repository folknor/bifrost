//! IDLE / WebSocket holder.
//!
//! For protocols whose push surface is in-process (IMAP IDLE, JMAP
//! WebSocket, EWS streaming), exactly one scope at a time can hold the
//! single push subscription. The IdleHolder records which scope is
//! currently bound and offers a non-blocking handoff primitive.
//!
//! The actual IDLE wire work lives in the protocol crate's
//! `push_stream`; the multiplexer just decides which scope is the
//! "most active" claimant.

use std::sync::Mutex;

use bifrost_types::CursorScope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleSignal {
    /// Acquired the slot for the given scope.
    Acquired,
    /// Slot held by another scope; caller should poll instead.
    Busy,
    /// Released a slot the caller previously held.
    Released,
}

/// Records which scope currently holds the IDLE / WebSocket slot.
#[derive(Debug, Default)]
pub struct IdleHolder {
    held_by: Mutex<Option<CursorScope>>,
}

impl IdleHolder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to acquire the IDLE slot for `scope`. Returns `Acquired` if
    /// the caller now holds it, `Busy` otherwise.
    pub fn try_acquire(&self, scope: CursorScope) -> IdleSignal {
        let mut g = self.held_by.lock().expect("poisoned");
        match g.as_ref() {
            None => {
                *g = Some(scope);
                IdleSignal::Acquired
            }
            Some(existing) if existing == &scope => IdleSignal::Acquired,
            Some(_) => IdleSignal::Busy,
        }
    }

    /// Release the slot. No-op if the caller never held it.
    pub fn release(&self, scope: &CursorScope) -> IdleSignal {
        let mut g = self.held_by.lock().expect("poisoned");
        if matches!(g.as_ref(), Some(s) if s == scope) {
            *g = None;
            IdleSignal::Released
        } else {
            IdleSignal::Busy
        }
    }

    /// Which scope currently holds the slot, if any.
    #[must_use]
    pub fn current(&self) -> Option<CursorScope> {
        let g = self.held_by.lock().expect("poisoned");
        g.clone()
    }
}
