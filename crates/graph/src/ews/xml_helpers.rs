use quick_xml::Reader;
use quick_xml::escape::{resolve_xml_entity, unescape};
use quick_xml::events::{BytesRef, Event};

// SOAP envelope.

pub(crate) fn build_soap_envelope(body_xml: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"
               xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
  <soap:Header>
    <t:RequestServerVersion Version="Exchange2016"/>
  </soap:Header>
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"#
    )
}

/// Strip namespace prefixes from element names for easier matching.
/// e.g. "t:FolderId" -> "FolderId", "soap:Fault" -> "Fault"
pub(crate) fn strip_ns(name: &str) -> &str {
    match name.find(':') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

/// Escape the five XML metacharacters for embedding a value inside a
/// SOAP request body. Copied verbatim from the EWS request builders.
pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Well-known distinguished folder IDs that EWS treats specially.
/// `publicfoldersroot` is the entry point for the public-folder
/// hierarchy; any other (opaque base64) id is an ordinary `FolderId`.
pub(crate) fn is_distinguished_folder_id(id: &str) -> bool {
    matches!(
        id,
        "publicfoldersroot"
            | "inbox"
            | "drafts"
            | "sentitems"
            | "deleteditems"
            | "junkemail"
            | "outbox"
            | "calendar"
            | "contacts"
            | "tasks"
            | "notes"
            | "root"
            | "msgfolderroot"
    )
}

/// Read a named attribute off a start/empty tag, returning the empty
/// string when absent.
pub(crate) fn extract_attribute(e: &quick_xml::events::BytesStart<'_>, attr_name: &str) -> String {
    for attr in e.attributes().flatten() {
        if String::from_utf8_lossy(attr.key.as_ref()) == attr_name {
            return String::from_utf8_lossy(&attr.value).to_string();
        }
    }
    String::new()
}

// SOAP fault check.

pub(crate) fn check_soap_fault(xml: &str) -> Result<(), super::EwsError> {
    use super::SoapFaultCode;
    use bifrost_types::DiagnosticText;

    let mut reader = Reader::from_str(xml);
    let mut in_fault = false;
    let mut in_faultstring = false;
    let mut in_faultcode = false;
    let mut fault_message = String::new();
    let mut fault_code_raw = String::new();
    let mut buf = String::new();
    let mut xml_error: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if local == "Fault" {
                    in_fault = true;
                }
                if in_fault && local == "faultstring" {
                    in_faultstring = true;
                }
                if in_fault && local == "faultcode" {
                    in_faultcode = true;
                }
                buf.clear();
            }
            Ok(Event::Text(ref e)) => {
                if let Ok(raw) = std::str::from_utf8(e.as_ref())
                    && let Ok(text) = unescape(raw)
                {
                    buf.push_str(&text);
                }
            }
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_faultstring && local == "faultstring" {
                    fault_message = buf.trim().to_string();
                    in_faultstring = false;
                }
                if in_faultcode && local == "faultcode" {
                    fault_code_raw = buf.trim().to_string();
                    in_faultcode = false;
                }
                if local == "Fault" {
                    break;
                }
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                xml_error = Some(error.to_string());
                break;
            }
            _ => {}
        }
    }

    if let Some(error) = xml_error {
        return Err(super::EwsError::MalformedXml(DiagnosticText::support_only(
            format!("EWS SOAP fault parse failed: {error}"),
        )));
    }

    if in_fault || !fault_message.is_empty() || !fault_code_raw.is_empty() {
        let code = if fault_code_raw.is_empty() {
            SoapFaultCode::Unknown
        } else {
            SoapFaultCode::parse(&fault_code_raw)
        };
        let detail_text = if fault_message.is_empty() {
            "Unknown SOAP fault".to_string()
        } else {
            fault_message
        };
        return Err(super::EwsError::SoapFault {
            code,
            detail: DiagnosticText::support_only(detail_text),
        });
    }

    Ok(())
}

/// Inspect a 200-OK EWS response body for an application-level error
/// carried in the canonical `ResponseClass="Error"` / `<m:ResponseCode>`
/// shape (distinct from a SOAP `<Fault>`, which `check_soap_fault`
/// handles). EWS reports the overwhelming majority of operation failures
/// (`ErrorAccessDenied`, `ErrorServerBusy`, `ErrorItemNotFound`, and so
/// on) this way inside an HTTP 200, not as a SOAP fault. Without this the
/// failed response parses to an empty success: a revoked or throttled
/// public folder would read as "zero items" and the deletion reconcile
/// would emit a spurious mass-`Destroyed` while the cursor advanced.
///
/// The first `ResponseMessage` whose `ResponseClass="Error"` wins; its
/// `<m:ResponseCode>` token is mapped through `SoapFaultCode::parse`
/// (the same `ErrorXxx` vocabulary) and the optional `<m:MessageText>`
/// becomes the support-only detail. A `ResponseClass="Warning"` is not
/// an error and is ignored. `NoError` / `Success` pass through.
pub(crate) fn check_response_error(xml: &str) -> Result<(), super::EwsError> {
    use super::SoapFaultCode;
    use bifrost_types::DiagnosticText;

    let mut reader = Reader::from_str(xml);
    let mut in_error_message = false;
    let mut in_response_code = false;
    let mut in_message_text = false;
    let mut response_code = String::new();
    let mut message_text = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if extract_attribute(e, "ResponseClass") == "Error" {
                    in_error_message = true;
                }
                if in_error_message && local == "ResponseCode" {
                    in_response_code = true;
                }
                if in_error_message && local == "MessageText" {
                    in_message_text = true;
                }
                buf.clear();
            }
            Ok(Event::Text(ref e)) => {
                if let Ok(raw) = std::str::from_utf8(e.as_ref())
                    && let Ok(text) = unescape(raw)
                {
                    buf.push_str(&text);
                }
            }
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_response_code && local == "ResponseCode" {
                    response_code = buf.trim().to_string();
                    in_response_code = false;
                }
                if in_message_text && local == "MessageText" {
                    message_text = buf.trim().to_string();
                    in_message_text = false;
                }
                // The first errored ResponseMessage decides the
                // classification; stop once we have its code.
                if in_error_message && !response_code.is_empty() {
                    break;
                }
                buf.clear();
            }
            Ok(Event::Eof) => break,
            // A parse error here is reported as malformed XML, identical
            // to the per-operation parsers, rather than silently treated
            // as success.
            Err(error) => {
                return Err(super::EwsError::MalformedXml(DiagnosticText::support_only(
                    format!("EWS response error scan failed: {error}"),
                )));
            }
            _ => {}
        }
    }

    if in_error_message && !response_code.is_empty() {
        // `NoError` on an Error-classed message is contradictory; treat
        // any non-NoError code as the fault. `SoapFaultCode::parse` maps
        // the EWS `ErrorXxx` vocabulary onto the shared taxonomy and
        // collapses the unrecognized rest onto `Unknown`.
        if response_code != "NoError" {
            let detail = if message_text.is_empty() {
                format!("EWS ResponseCode {response_code}")
            } else {
                format!("{response_code}: {message_text}")
            };
            return Err(super::EwsError::SoapFault {
                code: SoapFaultCode::parse(&response_code),
                detail: DiagnosticText::support_only(detail),
            });
        }
    }

    Ok(())
}

// quick-xml 0.36+ emits Event::GeneralRef separately from Event::Text, so
// every parser that accumulates body text needs to fold these back in or
// `&lt;` and friends silently vanish.
pub(crate) fn push_general_ref(e: &BytesRef<'_>, buf: &mut String) {
    let Ok(name) = std::str::from_utf8(e.as_ref()) else {
        return;
    };
    if let Some(rest) = name.strip_prefix('#') {
        let codepoint = if let Some(hex) = rest.strip_prefix(['x', 'X']) {
            u32::from_str_radix(hex, 16).ok()
        } else {
            rest.parse::<u32>().ok()
        };
        if let Some(c) = codepoint.and_then(char::from_u32) {
            buf.push(c);
        }
    } else if let Some(s) = resolve_xml_entity(name) {
        buf.push_str(s);
    }
}
