//! The pieces both push arms and the dispatcher share: the webhook endpoint
//! configuration, the two `Unsupported(PushSubscribe)` refusals, and the
//! subscription handle mint.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    ErrorScope, Protocol, Provider, RequestCause, SubscriptionHandle, TransmissionState,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Clone)]
pub(crate) struct PushEndpoint {
    pub(crate) webhook_url: String,
    /// A consumer-owned account-wide secret carried in every Graph webhook
    /// subscription so its out-of-process receiver can validate clientState.
    ///
    /// Mandatory. The alternative was a per-resource random value minted
    /// inside `create_subscription` and dropped on the floor, which produced
    /// subscriptions no receiver could authenticate - a secret nobody holds
    /// is not a secret, it is an unvalidated webhook with a field filled in.
    pub(crate) client_state: String,
}

pub(super) fn unsupported_push_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// The same refusal, correlated to the scope that caused it. A per-scope lane
/// entry the caller cannot attribute is not a report it can act on: it would
/// know a scope was declined without knowing which registration to drop.
pub(super) fn unsupported_push_scope_error(scope: CursorScope) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(ErrorScope::Cursor(scope))
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn new_handle() -> Result<SubscriptionHandle, AccountError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        // RNG failure is a host-environment problem. Surface it as a
        // transport "Network" failure with `transmission_state:
        // Unsent` (no bytes left the process) so the recovery mapping
        // classifies it as a retryable client-side issue.
        let net = bifrost_net::Error::Network {
            message: format!("RNG failed: {error}"),
            transmission_state: TransmissionState::Unsent,
            source: None,
        };
        into_account_error(
            crate::error::GraphError::Net(net),
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        )
    })?;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(SubscriptionHandle(out))
}
