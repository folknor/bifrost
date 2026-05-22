mod client;
mod xml_helpers;

use bifrost_net::AccountNet;

use self::xml_helpers::*;

const EWS_URL: &str = "https://outlook.office365.com/EWS/Exchange.asmx";

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
        assert!(err.contains("schema validation"));
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
