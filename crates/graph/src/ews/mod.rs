mod client;
mod xml_helpers;

use bifrost_net::AccountNet;

use self::xml_helpers::*;

const EWS_URL: &str = "https://outlook.office365.com/EWS/Exchange.asmx";

/// Structured EWS transport or protocol error.
///
/// Used by `EwsClient::execute` so callers receive structured evidence
/// rather than a formatted string. Callers that only need a display
/// message can call `.to_string()` via the `Display` impl.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum EwsError {
    /// The HTTP request failed or the server returned a non-success
    /// status. Carries the formatted network message.
    Transport(String),
    /// The server returned a SOAP fault. `message` is the
    /// `<faultstring>` text, or "Unknown SOAP fault" when the element
    /// is absent.
    SoapFault { message: String },
}

impl std::fmt::Display for EwsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EwsError::Transport(msg) => write!(f, "EWS transport error: {msg}"),
            EwsError::SoapFault { message } => write!(f, "EWS SOAP fault: {message}"),
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
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("schema validation"),
            "unexpected fault message: {err}"
        );
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
}
