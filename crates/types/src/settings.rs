//! Account settings: identities, vacation responder, quota.
//!
//! All shapes are protocol-agnostic. Each Account impl maps the
//! provider's native shape onto these structs - JMAP `Identity/get`,
//! Gmail `users.settings.sendAs.list`, Graph `me/mailboxSettings`.

use std::time::SystemTime;

use crate::compose::{Address, IdentityId};

/// One sending identity exposed by `Account::identities_list`.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Identity` values directly.
#[derive(Debug, Clone)]
pub struct Identity {
    pub id: IdentityId,
    /// Display name. Often empty for protocols that store only an
    /// address (IMAP).
    pub name: String,
    /// Email address for this identity.
    pub address: String,
    /// Plain-text signature, when set.
    pub signature_text: Option<String>,
    /// HTML signature, when set.
    pub signature_html: Option<String>,
    /// Reply-To address, when configured.
    pub reply_to: Option<Address>,
    /// Whether this is the account's default identity.
    pub is_default: bool,
}

/// Partial-update patch for `Account::identity_update`. Same
/// double-`Option` convention as `DraftPatch`: outer `None` means
/// "do not change", inner `None` means "clear".
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct IdentityPatch {
    pub name: Option<String>,
    pub signature_text: Option<Option<String>>,
    pub signature_html: Option<Option<String>>,
    pub reply_to: Option<Option<Address>>,
    pub is_default: Option<bool>,
}

/// Vacation / out-of-office responder configuration.
///
/// `is_enabled` is the master switch. The text bodies and the time
/// window are independent of the switch: a consumer can edit them
/// while the responder is off and the protocol stores them in
/// preparation for a future flip.
///
/// Not `#[non_exhaustive]` because consumers construct
/// `VacationConfig` to pass to `vacation_set`, and protocol Account
/// impls construct it as the return value of `vacation_get`. Both
/// sides need to fill in fields by name.
#[derive(Debug, Clone)]
pub struct VacationConfig {
    pub is_enabled: bool,
    /// Subject prefix the responder uses on its replies.
    pub subject: Option<String>,
    /// Plain-text auto-reply body.
    pub body_text: Option<String>,
    /// HTML auto-reply body.
    pub body_html: Option<String>,
    /// Window start. `None` means "start immediately when enabled".
    pub starts_at: Option<SystemTime>,
    /// Window end. `None` means "no end date".
    pub ends_at: Option<SystemTime>,
}

/// Storage quota readout from `Account::quota_get`.
///
/// `used` and `total` are in bytes. `total` is `None` when the
/// protocol exposes "X% used" rather than absolute totals (Graph's
/// `mailbox` resource).
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `QuotaInfo` directly.
#[derive(Debug, Clone, Copy)]
pub struct QuotaInfo {
    pub used_bytes: u64,
    pub total_bytes: Option<u64>,
}
