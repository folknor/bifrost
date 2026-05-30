use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, FilterDiagnostic, FilterDiagnosticSeverity,
    FilterScript, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};

use crate::blob::BlobRef;
use crate::core::SetCreate;
use crate::core::id::BlobId;
use crate::core::set::SetError;
use crate::sieve::validate::SieveScriptValidateRequest;
use crate::sieve::{
    Property as SieveProperty, SieveScriptGet, SieveScriptId, SieveScriptQuery, SieveScriptSet,
};
use crate::transport_reqwest::ReqwestTransport;

type SieveAccount = crate::account::Account<ReqwestTransport>;

const SIEVE_MIME: &str = "application/sieve";
const SIEVE_DOWNLOAD_NAME: &str = "filter.sieve";

#[inline]
fn to_acct_err(op: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(op))
}

fn unsupported(op: AccountOperation, detail: &'static str) -> AccountError {
    super::error::unsupported_error(op, None, detail)
}

fn require_sieve(
    account: Option<SieveAccount>,
    op: AccountOperation,
) -> Result<SieveAccount, AccountError> {
    account.ok_or_else(|| unsupported(op, "JMAP Sieve capability not available"))
}

pub(crate) fn list(
    sieve: Option<SieveAccount>,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FiltersList)?;
        let ids = account
            .call(SieveScriptQuery::new())
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?
            .into_ids();
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let response = account
            .call(SieveScriptGet::new().ids(ids).properties([
                SieveProperty::Id,
                SieveProperty::Name,
                SieveProperty::BlobId,
                SieveProperty::IsActive,
            ]))
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?;

        let mut filters = Vec::new();
        for mut script in response.into_list() {
            let id = script.take_id().into_string();
            let body = match script.blob_id() {
                Some(blob_id) => {
                    download_script_body(&account, blob_id, AccountOperation::FiltersList).await?
                }
                None => String::new(),
            };
            filters.push(ServerFilter::Script(FilterScript {
                id: ServerFilterId(id),
                name: script.name().map(str::to_owned),
                language: ScriptLanguage::Sieve,
                body,
                is_active: script.is_active(),
            }));
        }
        Ok(filters)
    })
}

pub(crate) fn create(
    sieve: Option<SieveAccount>,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<ServerFilterId, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterCreate)?;
        let ServerFilterCreate::Script(script) = filter else {
            return Err(unsupported(
                AccountOperation::FilterCreate,
                "JMAP Sieve only supports literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(unsupported(
                AccountOperation::FilterCreate,
                "JMAP Sieve only supports Sieve scripts",
            ));
        }

        let blob_id = upload_script_body(&account, script.body, AccountOperation::FilterCreate)
            .await?
            .into_string();
        let mut create = crate::sieve::SieveScriptCreate::new(None);
        create.blob_id(BlobId::new(blob_id));
        if let Some(name) = script.name {
            create.name(name);
        }

        let mut set = SieveScriptSet::new();
        let create_id = set.create_item(create);
        if script.is_active {
            set = set.on_success_activate_script(create_id.clone());
        }

        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        let mut created = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        Ok(ServerFilterId(created.take_id().into_string()))
    })
}

pub(crate) fn update(
    sieve: Option<SieveAccount>,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterUpdate)?;
        let ServerFilterPatch::Script(patch) = patch else {
            return Err(unsupported(
                AccountOperation::FilterUpdate,
                "JMAP Sieve only supports literal script filters",
            ));
        };

        let id = SieveScriptId::new(filter.0);
        let mut set = SieveScriptSet::new();
        let mut script_patch = crate::sieve::SieveScriptPatch::default();
        let mut has_patch = false;

        if let Some(name) = patch.name {
            // A clear (Some(None)) collapses to an empty name: Sieve
            // script names are not nullable, so the empty string is the
            // closest representation of "unset".
            script_patch.name(name.unwrap_or_default());
            has_patch = true;
        }
        if let Some(body) = patch.body {
            let blob_id = upload_script_body(&account, body, AccountOperation::FilterUpdate)
                .await?
                .into_string();
            script_patch.blob_id(BlobId::new(blob_id));
            has_patch = true;
        }

        if has_patch {
            set.update_item(id.clone(), script_patch);
        }
        if let Some(is_active) = patch.is_active {
            if is_active {
                set = set.on_success_activate_script_id(id.clone());
            } else {
                // Sieve activation is global (at most one active script),
                // so deactivation targets "no active script" rather than
                // this id specifically.
                set = set.on_success_deactivate_script(true);
            }
        }
        if !has_patch && patch.is_active.is_none() {
            return Ok(());
        }

        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        if has_patch {
            response
                .updated(&id)
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        } else {
            response
                .unwrap_update_errors()
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        Ok(())
    })
}

pub(crate) fn delete(
    sieve: Option<SieveAccount>,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterDelete)?;
        let id = SieveScriptId::new(filter.0);
        let mut response = account
            .call(SieveScriptSet::new().destroy([id.clone()]))
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))?;
        response
            .destroyed(&id)
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn validate(
    sieve: Option<SieveAccount>,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterValidate)?;
        let ServerFilterCreate::Script(script) = filter else {
            return Err(unsupported(
                AccountOperation::FilterValidate,
                "JMAP Sieve only validates literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(unsupported(
                AccountOperation::FilterValidate,
                "JMAP Sieve only validates Sieve scripts",
            ));
        }

        // SieveScript/validate takes a blob, so the body is uploaded
        // first. The blob is never referenced by a stored script, so the
        // server garbage-collects it.
        let blob_id = upload_script_body(&account, script.body, AccountOperation::FilterValidate)
            .await?
            .into_string();
        let response = account
            .call(SieveScriptValidateRequest::new(BlobId::new(blob_id)))
            .await
            .map_err(to_acct_err(AccountOperation::FilterValidate))?;
        Ok(validation_from_error(response.into_error()))
    })
}

fn validation_from_error(error: Option<SetError<String>>) -> FilterValidation {
    match error {
        None => FilterValidation::default(),
        Some(error) => FilterValidation {
            diagnostics: vec![FilterDiagnostic {
                severity: FilterDiagnosticSeverity::Error,
                message: error.to_string(),
                line: None,
                column: None,
            }],
        },
    }
}

async fn upload_script_body(
    account: &SieveAccount,
    body: String,
    op: AccountOperation,
) -> Result<BlobId, AccountError> {
    let blob = account
        .upload(body.into_bytes(), Some(SIEVE_MIME))
        .await
        .map_err(to_acct_err(op))?;
    Ok(blob.blob_id)
}

async fn download_script_body(
    account: &SieveAccount,
    blob_id: &BlobId,
    op: AccountOperation,
) -> Result<String, AccountError> {
    let blob = BlobRef::new(account.id().clone(), blob_id.clone())
        .with_name(SIEVE_DOWNLOAD_NAME)
        .with_content_type(SIEVE_MIME);
    let bytes = account
        .client()
        .download(&blob)
        .await
        .map_err(to_acct_err(op))?;
    String::from_utf8(bytes.to_vec()).map_err(|err| {
        to_acct_err(op)(crate::Error::NotParsable(format!(
            "Sieve script body is not UTF-8: {err}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_maps_sieve_set_error_to_error_diagnostic() {
        let error = serde_json::from_str::<SetError<String>>(
            r#"{"type":"invalidScript","description":"line 1: bad command"}"#,
        )
        .expect("set error decodes");
        let validation = validation_from_error(Some(error));
        assert!(!validation.is_valid());
        assert_eq!(validation.diagnostics.len(), 1);
        assert_eq!(
            validation.diagnostics[0].severity,
            FilterDiagnosticSeverity::Error
        );
        assert!(
            validation.diagnostics[0]
                .message
                .contains("line 1: bad command")
        );
    }

    #[test]
    fn validate_success_has_no_diagnostics() {
        let validation = validation_from_error(None);
        assert!(validation.is_valid());
        assert!(validation.diagnostics.is_empty());
    }
}
