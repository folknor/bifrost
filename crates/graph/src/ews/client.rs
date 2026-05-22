use super::{EWS_URL, EwsClient, build_soap_envelope, check_soap_fault};

impl EwsClient {
    pub(crate) fn new(net: bifrost_net::AccountNet) -> Self {
        Self {
            net,
            ews_url: EWS_URL.to_string(),
        }
    }

    /// Execute a raw EWS SOAP request. Wraps `body_xml` in the SOAP
    /// envelope, sends it, checks for SOAP faults, and returns the
    /// response body.
    pub(crate) async fn execute(&self, body_xml: &str) -> Result<String, String> {
        let envelope = build_soap_envelope(body_xml);
        let resp = self
            .net
            .post(&self.ews_url)
            .header("Content-Type", "text/xml; charset=utf-8")
            .body(bytes::Bytes::from(envelope))
            .send()
            .await
            .map_err(|e| format!("EWS request failed: {e}"))?;

        let status = resp.status();
        if !status.is_success() {
            let body = String::from_utf8_lossy(resp.body.as_ref());
            return Err(format!("EWS returned {status}: {body}"));
        }

        let xml = String::from_utf8_lossy(resp.body.as_ref()).into_owned();
        check_soap_fault(&xml)?;
        Ok(xml)
    }
}
