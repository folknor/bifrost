use super::{EWS_URL, EwsClient, EwsError, build_soap_envelope, check_soap_fault};

impl EwsClient {
    pub(crate) fn new(net: bifrost_net::AccountNet) -> Self {
        Self {
            net,
            ews_url: EWS_URL.to_string(),
        }
    }

    /// Execute a raw EWS SOAP request. Wraps `body_xml` in the SOAP
    /// envelope, sends it, checks for SOAP faults, and returns the
    /// response body as a string.
    ///
    /// Transport failures preserve the `bifrost_net::Error` so the
    /// account boundary classifies them through
    /// `bifrost_net::into_account_error` with `Protocol::Ews` context.
    /// HTTP error statuses carry the status + raw body; SOAP faults
    /// carry the structured `SoapFaultCode` plus `<faultstring>`
    /// detail.
    pub(crate) async fn execute(&self, body_xml: &str) -> Result<String, EwsError> {
        let envelope = build_soap_envelope(body_xml);
        let resp = self
            .net
            .post(&self.ews_url)
            .header("Content-Type", "text/xml; charset=utf-8")
            .body(bytes::Bytes::from(envelope))
            .send()
            .await
            .map_err(EwsError::Transport)?;

        let status = resp.status();
        if !status.is_success() {
            return Err(EwsError::HttpStatus {
                status,
                body: resp.body,
            });
        }

        let xml = String::from_utf8_lossy(resp.body.as_ref()).into_owned();
        check_soap_fault(&xml)?;
        Ok(xml)
    }
}
