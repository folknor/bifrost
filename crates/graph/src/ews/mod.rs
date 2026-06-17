mod client;
mod ops;
mod parse;
mod xml_helpers;

use bifrost_net::AccountNet;
use bifrost_types::DiagnosticText;

pub(crate) use self::parse::*;
pub(crate) use self::xml_helpers::*;

const EWS_URL: &str = "https://outlook.office365.com/EWS/Exchange.asmx";

/// EWS request routing headers. Public-folder operations route by
/// `X-AnchorMailbox` (the hierarchy or content mailbox SMTP address) and
/// `X-PublicFolderMailbox` (the mailbox server for hierarchy ops, or the
/// content mailbox for content ops). Both are `None` for ordinary
/// primary-mailbox EWS calls (the streaming-notification path).
#[derive(Debug, Clone, Default)]
pub(crate) struct EwsHeaders {
    pub(crate) anchor_mailbox: Option<String>,
    pub(crate) public_folder_mailbox: Option<String>,
}

impl EwsHeaders {
    /// Materialize the present routing headers as `(name, value)` pairs
    /// the request builder attaches. Extracted as a pure helper so the
    /// header decision is unit-pinnable without a live request (no
    /// mock-server per repo rules).
    pub(crate) fn pairs(&self) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        if let Some(anchor) = &self.anchor_mailbox {
            out.push(("X-AnchorMailbox", anchor.clone()));
        }
        if let Some(pf) = &self.public_folder_mailbox {
            out.push(("X-PublicFolderMailbox", pf.clone()));
        }
        out
    }
}

/// Structured EWS transport or protocol error.
///
/// Used by `EwsClient::execute` so callers receive structured evidence
/// rather than a formatted string. Translated into `AccountError` via
/// `account::graph_error::ews_error_to_account_error`.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum EwsError {
    /// The HTTP request failed. Carries the underlying
    /// `bifrost_net::Error` so the account boundary can route through
    /// `bifrost_net::into_account_error` with `Protocol::Ews` context.
    Transport(bifrost_net::Error),

    /// The HTTP request completed with a non-success status. Carries
    /// the status code and the response body for diagnostics.
    HttpStatus {
        status: reqwest::StatusCode,
        body: bytes::Bytes,
    },

    /// The server returned a SOAP fault inside an otherwise 2xx
    /// response. `code` carries the structured `<faultcode>` token;
    /// `detail` carries the `<faultstring>` text (support-only).
    SoapFault {
        code: SoapFaultCode,
        detail: DiagnosticText,
    },

    /// XML body could not be parsed. Surfaces at the boundary as
    /// `WireCause::MalformedResponse { protocol: Protocol::Ews, .. }`.
    MalformedXml(DiagnosticText),
}

impl std::fmt::Display for EwsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "EWS transport error: {error}"),
            Self::HttpStatus { status, .. } => write!(f, "EWS HTTP {status}"),
            Self::SoapFault { code, detail } => {
                write!(f, "EWS SOAP fault {code:?}: {}", detail.as_str())
            }
            Self::MalformedXml(detail) => write!(f, "EWS malformed XML: {}", detail.as_str()),
        }
    }
}

/// SOAP fault code as carried in `<soap:Fault><faultcode>`. EWS uses
/// the standard SOAP 1.1 codes plus Microsoft-specific
/// `ErrorAccessDenied` / `ErrorServerBusy` / etc. inside `<detail>`;
/// for classification we collapse onto the SOAP 1.1 set and keep the
/// detail string in the carrier.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum SoapFaultCode {
    /// `VersionMismatch` - protocol-level disagreement on SOAP version.
    VersionMismatch,
    /// `MustUnderstand` - a `mustUnderstand` header was not understood.
    MustUnderstand,
    /// `Client` - request was malformed or lacked required information.
    Client,
    /// `Server` - server failed to process the request (5xx-equivalent).
    Server,
    /// Microsoft EWS: caller lacks the rights to perform the operation.
    /// Routes to `Authorization(PermissionDenied)`.
    ErrorAccessDenied,
    /// Microsoft EWS: caller cannot impersonate the target user. Routes
    /// to `Authorization(ConditionalAccessBlocked)` or
    /// `PermissionDenied` depending on context (we use
    /// `ConditionalAccessBlocked` since impersonation is a tenant-policy
    /// gate).
    ErrorImpersonateUserDenied,
    /// Microsoft EWS: backend is throttling. Routes to
    /// `Server(RateLimited)` so the engine respects the throttle hint.
    ErrorServerBusy,
    /// Microsoft EWS: target mailbox is temporarily unavailable. Routes
    /// to `Authorization(MailboxUnavailable { Transient })` so the
    /// engine retries.
    ErrorMailboxStoreUnavailable,
    /// Microsoft EWS: caller's mailbox is moving between databases.
    /// Routes to `Authorization(MailboxUnavailable { Transient })`.
    ErrorMailboxMoveInProgress,
    /// Microsoft EWS: target user / mailbox cannot be found. Routes to
    /// `NotFound(Mailbox)` so consumers route to the missing-mailbox UX.
    ErrorNonExistentMailbox,
    /// Microsoft EWS: the requested item no longer exists. Routes to
    /// `NotFound(Message)`.
    ErrorItemNotFound,
    /// Unrecognized fault code. Microsoft EWS uses many other
    /// `ErrorXxx` codes; the ones we don't classify explicitly land
    /// here and route to `Protocol(ContractViolation)`.
    Unknown,
}

impl SoapFaultCode {
    pub(crate) fn parse(raw: &str) -> Self {
        let local = raw.rsplit_once(':').map_or(raw, |(_, l)| l);
        match local {
            "VersionMismatch" => Self::VersionMismatch,
            "MustUnderstand" => Self::MustUnderstand,
            "Client" => Self::Client,
            "Server" => Self::Server,
            "ErrorAccessDenied" => Self::ErrorAccessDenied,
            "ErrorImpersonateUserDenied" => Self::ErrorImpersonateUserDenied,
            "ErrorServerBusy" => Self::ErrorServerBusy,
            "ErrorMailboxStoreUnavailable" => Self::ErrorMailboxStoreUnavailable,
            "ErrorMailboxMoveInProgress" => Self::ErrorMailboxMoveInProgress,
            "ErrorNonExistentMailbox" => Self::ErrorNonExistentMailbox,
            "ErrorItemNotFound" => Self::ErrorItemNotFound,
            _ => Self::Unknown,
        }
    }
}

pub(crate) struct EwsClient {
    net: AccountNet,
    ews_url: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn soap_envelope_wraps_body() {
        let body = r#"<m:FindFolder Traversal="Shallow"/>"#;
        let envelope = build_soap_envelope(body);
        assert!(envelope.contains("soap:Envelope"));
        assert!(envelope.contains("soap:Header"));
        assert!(envelope.contains("RequestServerVersion"));
        assert!(envelope.contains("Exchange2016"));
        assert!(envelope.contains("soap:Body"));
        assert!(envelope.contains(body));
    }

    #[test]
    fn soap_fault_detected() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <soap:Fault>
      <faultcode>soap:Client</faultcode>
      <faultstring>The request failed schema validation.</faultstring>
    </soap:Fault>
  </soap:Body>
</soap:Envelope>"#;

        let result = check_soap_fault(xml);
        assert!(result.is_err());
        match result.unwrap_err() {
            EwsError::SoapFault { code, detail } => {
                assert_eq!(code, SoapFaultCode::Client);
                assert!(
                    detail.as_str().contains("schema validation"),
                    "unexpected fault detail: {}",
                    detail.as_str()
                );
            }
            other => panic!("expected SoapFault, got {other:?}"),
        }
    }

    #[test]
    fn no_soap_fault_passes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <m:FindFolderResponse>
      <m:ResponseMessages/>
    </m:FindFolderResponse>
  </soap:Body>
</soap:Envelope>"#;

        assert!(check_soap_fault(xml).is_ok());
    }

    // A 200-OK EWS body whose ResponseMessage is ResponseClass="Error"
    // carries the application failure in `<m:ResponseCode>`, NOT a SOAP
    // `<Fault>`. `check_response_error` must surface it so the response
    // does not parse to an empty success.
    #[test]
    fn response_class_error_access_denied_is_classified() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied. Check credentials and try again.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        match check_response_error(xml).expect_err("error response must classify") {
            EwsError::SoapFault { code, detail } => {
                assert_eq!(code, SoapFaultCode::ErrorAccessDenied);
                assert!(detail.as_str().contains("Access is denied"));
            }
            other => panic!("expected SoapFault, got {other:?}"),
        }
    }

    #[test]
    fn response_class_error_server_busy_is_classified() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:MessageText>The server is busy. Please try again later.</m:MessageText>
          <m:ResponseCode>ErrorServerBusy</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        match check_response_error(xml).expect_err("throttle response must classify") {
            EwsError::SoapFault { code, .. } => {
                assert_eq!(code, SoapFaultCode::ErrorServerBusy);
            }
            other => panic!("expected SoapFault, got {other:?}"),
        }
    }

    #[test]
    fn response_class_success_passes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        assert!(check_response_error(xml).is_ok());
    }
}
