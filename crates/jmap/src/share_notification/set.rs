//! ShareNotification is destroy-only - no create or update is permitted
//! (RFC 9670). Only `SetObject` is implemented, not `SetObjectCreatable`,
//! so `create()` and `update()` are unavailable at compile time.

use crate::{Get, Set, core::set::SetObject};

use super::ShareNotification;

impl SetObject for ShareNotification<Set> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObject for ShareNotification<Get> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        None
    }
}
