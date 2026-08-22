mod client;
mod ops;
mod parse;
mod xml_helpers;

use bifrost_net::AccountNet;
use bifrost_types::DiagnosticText;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

pub(crate) use self::parse::*;
pub(crate) use self::xml_helpers::*;

/// The EWS SOAP endpoint under a given Outlook origin. Derived rather than
/// hardcoded so the harness api-base override reaches EWS too (the
/// production origin lives on `outlook.office365.com`, not on the Graph
/// host, so redirecting the Graph base alone left EWS pointed at the real
/// service).
pub(crate) fn ews_url(outlook_base: &str) -> String {
    format!("{}/EWS/Exchange.asmx", outlook_base.trim_end_matches('/'))
}

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
    /// Microsoft EWS: a streaming subscription or its watermark expired,
    /// was deleted, or is otherwise no longer usable. The worker reconnects
    /// and creates a fresh subscription.
    ErrorStreamingSubscriptionInvalid,
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
            "ErrorSubscriptionNotFound"
            | "ErrorInvalidSubscription"
            | "ErrorSubscriptionUnsubscribed"
            | "ErrorInvalidWatermark"
            | "ErrorInternalServerTransientError" => Self::ErrorStreamingSubscriptionInvalid,
            _ => Self::Unknown,
        }
    }
}

pub(crate) struct EwsClient {
    net: AccountNet,
    ews_url: String,
}

/// A streaming EWS response body. GetStreamingEvents keeps one HTTP response
/// open and emits response-message frames as Exchange has notifications, so
/// this must stay chunked all the way to the account worker.
pub(crate) type EwsBodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, EwsError>> + Send>>;

/// The seam between the EWS worker loops and the wire.
///
/// `EwsClient::execute` is the single funnel every EWS request in this
/// crate goes through, so a scripted implementation of this one method is
/// enough to drive the whole Subscribe / GetStreamingEvents / Unsubscribe
/// cycle hermetically. The streaming worker takes `impl EwsExecute`
/// instead of the concrete client for exactly that reason; `EwsClient` is
/// the production implementation.
pub(crate) trait EwsExecute: Send + Sync {
    fn execute(
        &self,
        body_xml: &str,
        headers: &EwsHeaders,
    ) -> impl Future<Output = Result<String, EwsError>> + Send;

    /// Open an EWS streaming response. The default keeps existing scripted
    /// doubles source-compatible by treating their buffered answer as one
    /// chunk; production overrides it with the transport byte stream.
    fn execute_streaming(
        &self,
        body_xml: &str,
        headers: &EwsHeaders,
    ) -> impl Future<Output = Result<EwsBodyStream, EwsError>> + Send {
        async move {
            let response = self.execute(body_xml, headers).await?;
            Ok(Box::pin(futures::stream::once(
                async move { Ok(Bytes::from(response)) },
            )) as EwsBodyStream)
        }
    }
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

    /// The per-answer id count is what makes the whole-response error scan
    /// exact. `m:`-namespaced collections are per-item surfaces; a body
    /// naming two ids gets two verdicts back and cannot be answered once.
    #[test]
    fn per_answer_ids_counts_each_entry_of_an_m_namespaced_collection() {
        let one = r#"<m:GetItem><m:ItemIds><t:ItemId Id="a"/></m:ItemIds></m:GetItem>"#;
        assert_eq!(per_answer_request_ids(one), 1);

        let two = r#"<m:GetItem>
  <m:ItemIds>
    <t:ItemId Id="a"/>
    <t:ItemId Id="b"/>
  </m:ItemIds>
</m:GetItem>"#;
        assert_eq!(per_answer_request_ids(two), 2);

        // A non-self-closing id element is still one id, not two.
        let expanded = r#"<m:GetStreamingEvents><m:SubscriptionIds><t:SubscriptionId>s</t:SubscriptionId></m:SubscriptionIds></m:GetStreamingEvents>"#;
        assert_eq!(per_answer_request_ids(expanded), 1);

        // No collection at all, and an empty collection.
        assert_eq!(per_answer_request_ids("<m:GetItem/>"), 0);
        assert_eq!(
            per_answer_request_ids("<m:GetItem><m:ItemIds/></m:GetItem>"),
            0
        );
    }

    /// Subscribe's folder set is `t:FolderIds`, not `m:FolderIds`: it is the
    /// subscription's scope and EWS answers it with a single
    /// `SubscribeResponseMessage` regardless of how many folders it names.
    /// Counting it would make the guard reject a legitimate request.
    #[test]
    fn per_answer_ids_ignores_the_subscription_folder_set() {
        let body = r#"<m:Subscribe>
  <m:StreamingSubscriptionRequest>
    <t:FolderIds>
      <t:FolderId Id="f1"/>
      <t:FolderId Id="f2"/>
      <t:FolderId Id="f3"/>
    </t:FolderIds>
  </m:StreamingSubscriptionRequest>
</m:Subscribe>"#;
        assert_eq!(per_answer_request_ids(body), 0);
    }

    /// The guard lives in the envelope builder because that is the single
    /// funnel every EWS request passes through, so a future multi-item body
    /// trips it at construction time - no live transport needed.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "more than one per-answer id")]
    fn the_envelope_refuses_a_multi_item_body_in_debug_builds() {
        let _ = build_soap_envelope(
            r#"<m:GetItem><m:ItemIds><t:ItemId Id="a"/><t:ItemId Id="b"/></m:ItemIds></m:GetItem>"#,
        );
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

    /// `ResponseClass="Error"` and `ResponseCode="NoError"` are mutually
    /// exclusive. The pair says the operation failed but refuses to say how,
    /// which is the same position an error-classed message with no code at
    /// all leaves us in: a failure we cannot classify, never a success.
    #[test]
    fn error_class_carrying_no_error_is_malformed() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;

        match check_response_error(xml).expect_err("contradictory response must fail") {
            EwsError::MalformedXml(detail) => {
                assert!(detail.as_str().contains("NoError"), "{}", detail.as_str());
                assert!(
                    detail.as_str().contains("FindItemResponseMessage"),
                    "{}",
                    detail.as_str()
                );
            }
            other => panic!("expected MalformedXml, got {other:?}"),
        }
    }

    /// The contradictory pair is unclassifiable, not authoritative: a later
    /// message that names a real code still decides the body, exactly as it
    /// does after a code-less error. Otherwise a multi-item response whose
    /// first message is contradictory would report `Protocol(ParseFailed)`
    /// and throw away an `ErrorAccessDenied` sitting right behind it.
    #[test]
    fn classifiable_error_after_a_no_error_contradiction_wins() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Error">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:GetItemResponseMessage>
        <m:GetItemResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;

        match check_response_error(xml).expect_err("error response must classify") {
            EwsError::SoapFault { code, .. } => assert_eq!(code, SoapFaultCode::ErrorAccessDenied),
            other => panic!("expected SoapFault, got {other:?}"),
        }
    }

    /// ... but a SUCCESS message behind it must not rehabilitate it, and its
    /// `NoError` must not be read as the contradictory message's code.
    #[test]
    fn a_success_message_after_a_no_error_contradiction_does_not_clear_it() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Error">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:GetItemResponseMessage>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;

        match check_response_error(xml).expect_err("contradictory response must fail") {
            EwsError::MalformedXml(detail) => {
                assert!(detail.as_str().contains("NoError"), "{}", detail.as_str());
            }
            other => panic!("expected MalformedXml, got {other:?}"),
        }
    }

    // An error-classed response message with no `<m:ResponseCode>` is a
    // FAILED response we cannot classify. Passing it through would hand the
    // body to the operation parsers, which read a missing result set as an
    // empty successful one, so a revoked folder's items would silently
    // vanish. It must be `MalformedXml`, and it must NOT borrow the code of
    // the following (warning-classed) message.
    #[test]
    fn incomplete_error_response_is_malformed_not_success() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:MessageText>Malformed response without a code.</m:MessageText>
        </m:FindItemResponseMessage>
        <m:FindItemResponseMessage ResponseClass="Warning">
          <m:ResponseCode>ErrorServerBusy</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;

        match check_response_error(xml).expect_err("incomplete error must not read as success") {
            EwsError::MalformedXml(detail) => {
                assert!(
                    detail.as_str().contains("FindItemResponseMessage"),
                    "unexpected detail: {}",
                    detail.as_str()
                );
            }
            other => panic!("expected MalformedXml, got {other:?}"),
        }
    }

    /// A complete error later in the same body outranks the malformed
    /// report: its code carries the real classification (here the
    /// scope-quarantining `ErrorAccessDenied`), which
    /// `Protocol(ParseFailed)` would throw away.
    #[test]
    fn complete_error_after_an_incomplete_one_wins() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Error">
          <m:MessageText>Malformed response without a code.</m:MessageText>
        </m:GetItemResponseMessage>
        <m:GetItemResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;

        match check_response_error(xml).expect_err("error response must classify") {
            EwsError::SoapFault { code, .. } => {
                assert_eq!(code, SoapFaultCode::ErrorAccessDenied);
            }
            other => panic!("expected SoapFault, got {other:?}"),
        }
    }

    /// The empty-`ResponseMessages` success shape must stay a success: the
    /// malformed-error path keys on `ResponseClass="Error"`, not on the
    /// absence of a code.
    #[test]
    fn warning_class_without_a_code_still_passes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Warning">
          <m:MessageText>Partial results.</m:MessageText>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;

        assert!(check_response_error(xml).is_ok());
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
