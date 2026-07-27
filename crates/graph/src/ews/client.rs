use super::{
    EwsClient, EwsError, EwsHeaders, build_soap_envelope, check_response_error, check_soap_fault,
    ews_url,
};

impl EwsClient {
    /// Build a client whose SOAP endpoint sits under `outlook_base` (the
    /// client's Autodiscover/EWS origin, which honors the harness api-base
    /// override).
    pub(crate) fn new(net: bifrost_net::AccountNet, outlook_base: &str) -> Self {
        Self {
            net,
            ews_url: ews_url(outlook_base),
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
    pub(crate) async fn execute(
        &self,
        body_xml: &str,
        headers: &EwsHeaders,
    ) -> Result<String, EwsError> {
        let envelope = build_soap_envelope(body_xml);
        let mut req = self
            .net
            .post(&self.ews_url)
            .header("Content-Type", "text/xml; charset=utf-8");
        for (name, value) in headers.pairs() {
            req = req.header(name, &value);
        }
        let resp = req
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
        // Two distinct error shapes ride inside a 200 OK: a SOAP
        // `<Fault>` (transport/envelope failure) and the canonical EWS
        // application error `ResponseClass="Error"` / `<m:ResponseCode>`.
        // Inspect both before any parser sees the body, so an
        // `ErrorAccessDenied` / `ErrorServerBusy` is classified instead
        // of degrading to an empty success.
        check_soap_fault(&xml)?;
        check_response_error(&xml)?;
        Ok(xml)
    }
}
