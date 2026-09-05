//! Builders shared by more than one half of the push suite.
//!
//! `state` / `expiring` are read by the webhook teardown tests and by every
//! renewal test; `email_scope` / `pending` / `ledger` / `converted` are read by
//! the EWS translation tests and by the dispatcher tests that drive a request
//! through the EWS arm. Keeping them in one module is what lets the split
//! avoid duplicating a fixture whose shape is itself an assertion.

use std::collections::HashMap;

use bifrost_types::{CursorScope, FolderId, ObjectType};

use crate::account::push::ews::{
    PendingEwsScope, TranslatedExchangeId, ews_subscribable_folder_id,
};
use crate::account::push::webhook::GraphSubscriptionState;

pub(super) fn state(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
    }
}

/// A subscription whose expiry is already past, so it is unconditionally
/// inside the renewal threshold.
pub(super) fn expiring(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2000-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
    }
}

pub(super) fn email_scope(folder: &str) -> CursorScope {
    CursorScope::FolderType {
        folder: FolderId(folder.to_string()),
        ty: ObjectType::Email,
    }
}

/// What `subscribe_ews` hands the translation phase: every scope already
/// validated and paired with the native id it contributes.
pub(super) fn pending(folders: &[&str]) -> Vec<PendingEwsScope> {
    folders
        .iter()
        .enumerate()
        .map(|(index, folder)| {
            let scope = email_scope(folder);
            let source_id =
                ews_subscribable_folder_id(&scope).expect("a primary folder scope is pending");
            (
                bifrost_types::BatchItemId(index.to_string()),
                scope,
                source_id,
            )
        })
        .collect()
}

/// A fresh per-request ledger for a `reconcile_translated_ews_scopes`
/// call made outside `push_subscribe`.
pub(super) fn ledger() -> bifrost_types::BatchOutcomeBuilder<CursorScope> {
    bifrost_types::BatchOutcomeBuilder::new()
}

/// No translation chunk's POST failed, for the reconciliation tests that
/// script an answered request.
pub(super) fn no_chunk_failures() -> HashMap<String, bifrost_types::AccountError> {
    HashMap::new()
}

pub(super) fn converted(source_id: &str, target_id: &str) -> TranslatedExchangeId {
    TranslatedExchangeId {
        source_id: source_id.to_string(),
        target_id: Some(target_id.to_string()),
        error_details: None,
    }
}
