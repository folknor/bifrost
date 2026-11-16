//! Account filter-lane adapter for the sibling ManageSieve protocol.

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, Created, DiagnosticText, FilterScript,
    FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, Warning, WarningKind,
};

use super::error::ImapErrorContext;
use super::{ImapAccount, account_error_with};
use crate::Error;
pub use crate::sieve::ManageSieveConfig;
use crate::sieve::{Activation, Error as SieveError, ManageSieveClient, validation_error};

pub(crate) fn filters_list(
    account: ImapAccount,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FiltersList).await?;
        let scripts = client
            .list_scripts()
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?;
        let mut filters = Vec::with_capacity(scripts.len());
        for script in scripts {
            let body = client
                .get_script(&script.name)
                .await
                .map_err(to_acct_err(AccountOperation::FiltersList))?;
            filters.push(ServerFilter::Script(FilterScript {
                id: ServerFilterId(script.name.clone()),
                name: Some(script.name),
                language: ScriptLanguage::Sieve,
                body,
                is_active: script.is_active,
            }));
        }
        Ok(filters)
    })
}

pub(crate) fn filter_create(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<Created<ServerFilterId>, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Err(shape_unsupported(&account, AccountOperation::FilterCreate));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve only supports Sieve scripts",
            ));
        }
        let name = script.name.ok_or_else(|| {
            invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve script creation requires a name",
            )
        })?;
        let mut client = open_client(&account, AccountOperation::FilterCreate).await?;
        let advisory = client
            .put_script(&name, &script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        let activation = if script.is_active {
            Some(client.set_active(Some(&name)).await)
        } else {
            None
        };
        Ok(created_after_save(name, advisory, activation))
    })
}

/// The answer to a create whose PUTSCRIPT the server accepted.
///
/// From the save on the filter exists, so the answer is `Created` whatever
/// activation does: an Outlook user whose rule was saved expects "saved",
/// never a failure that invites saving it again. What did not go to plan
/// rides along as warnings, in the order it happened.
fn created_after_save(
    name: String,
    advisory: Option<String>,
    activation: Option<Activation>,
) -> Created<ServerFilterId> {
    let mut created = Created::identified(ServerFilterId(name));
    if let Some(text) = advisory {
        created = created.with_warning(advisory_warning(text));
    }
    match activation {
        None | Some(Activation::Done { advisory: None }) => created,
        Some(Activation::Done {
            advisory: Some(text),
        }) => created.with_warning(advisory_warning(text)),
        Some(Activation::Refused(error)) => created.with_warning(activation_warning(
            "The filter was saved, but the server refused to make it the active filter",
            error,
        )),
        Some(Activation::Unconfirmed(error)) => created.with_warning(activation_warning(
            "The filter was saved, but whether it is now the active filter could not be \
             confirmed",
            error,
        )),
    }
}

/// A server's `OK (WARNINGS)` on a save. The user saving a rule expects to
/// read the same warnings `filter_validate` would have shown, so the
/// server's text is user-safe: it is compiler output about the user's own
/// script, shown to that user.
fn advisory_warning(text: String) -> Warning {
    let warning = Warning::user_safe(
        WarningKind::ServerAdvisory,
        "The filter was saved, and the server reported warnings about it",
    );
    if text.trim().is_empty() {
        warning
    } else {
        warning.with_protocol_detail(DiagnosticText::user_safe(text))
    }
}

/// An activation that did not complete after the save did. No claim about
/// the resulting active state is made: a replaced script may have kept its
/// earlier status. The activation leg's error is attached; its recovery
/// advice concerns activation, never the save.
fn activation_warning(message: &str, error: SieveError) -> Warning {
    Warning::user_safe(WarningKind::FilterActivationIncomplete, message)
        .with_error(to_acct_err(AccountOperation::FilterCreate)(error))
}

pub(crate) fn filter_update(
    account: ImapAccount,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ServerFilterPatch::Script(patch) = patch else {
            return Err(shape_unsupported(&account, AccountOperation::FilterUpdate));
        };
        if patch.name.is_some() {
            return Err(invalid(
                AccountOperation::FilterUpdate,
                "IMAP ManageSieve script names are filter ids and cannot be patched",
            ));
        }
        // `filter_update` returns no success payload, so a server's
        // `OK (WARNINGS)` on either leg has no channel here and is dropped.
        let mut client = open_client(&account, AccountOperation::FilterUpdate).await?;
        if let Some(body) = patch.body {
            client
                .put_script(&filter.0, &body)
                .await
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        if let Some(is_active) = patch.is_active {
            let active = is_active.then_some(filter.0.as_str());
            client
                .set_active(active)
                .await
                .into_result()
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        Ok(())
    })
}

pub(crate) fn filter_delete(
    account: ImapAccount,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FilterDelete).await?;
        client
            .delete_script(&filter.0)
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn filter_validate(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Ok(validation_error(
                "IMAP ManageSieve only validates literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Ok(validation_error(
                "IMAP ManageSieve only validates Sieve scripts",
            ));
        }
        let mut client = open_client(&account, AccountOperation::FilterValidate).await?;
        client
            .check_script(&script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterValidate))
    })
}

async fn open_client(
    account: &ImapAccount,
    op: AccountOperation,
) -> Result<ManageSieveClient, AccountError> {
    let config = account
        .config
        .sieve
        .clone()
        .ok_or_else(|| super::error::unsupported(op))?;
    ManageSieveClient::connect(
        &config,
        &account.config.credentials,
        &account.config.auth_policy,
        account.bandwidth_budget.clone(),
        account.pool.sieve_meter(),
    )
    .await
    .map_err(to_acct_err(op))
}

fn to_acct_err(op: AccountOperation) -> impl Fn(SieveError) -> AccountError {
    move |err| super::error::sieve_account_error(err, ImapErrorContext::sieve(op))
}

/// A filter shape ManageSieve cannot carry. Named on the ManageSieve leg when
/// one is configured; an account without one has no filter leg, so the
/// refusal stays the IMAP account's.
fn shape_unsupported(account: &ImapAccount, op: AccountOperation) -> AccountError {
    if account.config.sieve.is_some() {
        super::error::sieve_unsupported(op)
    } else {
        super::error::unsupported(op)
    }
}

/// Input the ManageSieve leg refuses before any request.
fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    let detail: String = detail.into();
    account_error_with(
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(detail.into())),
        ImapErrorContext::sieve(op),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sieve::SieveResponseCode;

    fn kinds(created: &Created<ServerFilterId>) -> Vec<WarningKind> {
        created
            .warnings()
            .iter()
            .map(|warning| warning.kind.clone())
            .collect()
    }

    #[test]
    fn a_saved_filter_is_created_whatever_activation_does() {
        let refused = SieveError::Sieve {
            code: Some(SieveResponseCode::TryLater),
            message: "busy".into(),
        };
        for (activation, expected) in [
            (None, vec![]),
            (Some(Activation::Done { advisory: None }), vec![]),
            (
                Some(Activation::Refused(refused)),
                vec![WarningKind::FilterActivationIncomplete],
            ),
            (
                Some(Activation::Unconfirmed(SieveError::Timeout)),
                vec![WarningKind::FilterActivationIncomplete],
            ),
        ] {
            let created = created_after_save("main".into(), None, activation);
            assert_eq!(created.id().map(|id| id.0.as_str()), Some("main"));
            assert_eq!(kinds(&created), expected);
            for warning in created.warnings() {
                let error = warning.error.as_ref().expect("activation error attached");
                assert_eq!(error.operation(), Some(AccountOperation::FilterCreate));
            }
        }
    }

    #[test]
    fn refused_and_unconfirmed_activation_say_different_things() {
        let message = |activation| {
            created_after_save("main".into(), None, Some(activation)).warnings()[0]
                .message
                .as_str()
                .to_owned()
        };
        let refused = message(Activation::Refused(SieveError::Sieve {
            code: None,
            message: "no".into(),
        }));
        let unconfirmed = message(Activation::Unconfirmed(SieveError::Timeout));
        assert!(refused.contains("refused"), "{refused}");
        assert!(
            unconfirmed.contains("could not be confirmed"),
            "{unconfirmed}"
        );
    }

    #[test]
    fn server_warnings_on_save_and_activation_reach_the_user_in_order() {
        let created = created_after_save(
            "main".into(),
            Some("line 2: deprecated".into()),
            Some(Activation::Done {
                advisory: Some(String::new()),
            }),
        );
        assert_eq!(
            kinds(&created),
            vec![WarningKind::ServerAdvisory, WarningKind::ServerAdvisory]
        );
        let detail = created.warnings()[0]
            .protocol_detail
            .as_ref()
            .expect("server text kept");
        assert_eq!(detail.as_str(), "line 2: deprecated");
        assert_eq!(detail.visibility, bifrost_types::DetailVisibility::UserSafe);
        assert!(created.warnings()[1].protocol_detail.is_none());
    }
}
