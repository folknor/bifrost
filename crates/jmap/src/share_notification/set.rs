//! ShareNotification is destroy-only - no create or update is permitted
//! (RFC 9670). The Create/Patch types are uninhabitable enums, so
//! `SetRequest::create()` and `update()` do not resolve at compile time.
