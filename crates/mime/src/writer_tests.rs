use bifrost_types::compose::{Address, AttachmentInline, SendRequest};
use bifrost_types::error::{AccountErrorKind, RequestErrorKind};

use crate::{HeaderMap, HeaderValue, parse_content_type, send_request_to_rfc5322, write_header};

fn address(name: Option<String>, address: String) -> Address {
    Address { name, address }
}

fn render(request: &SendRequest) -> Result<String, bifrost_types::error::AccountError> {
    let sender = address(None, "me@example.com".into());
    send_request_to_rfc5322(request, &sender)
        .map(|message| String::from_utf8(message.raw).expect("UTF-8 wire"))
}

fn request() -> SendRequest {
    let mut request = SendRequest::default();
    request.to = vec![address(None, "you@example.com".into())];
    request
}

fn pairs(addresses: &[Address]) -> Vec<(Option<&str>, &str)> {
    addresses
        .iter()
        .map(|a| (a.name.as_deref(), a.address.as_str()))
        .collect()
}

fn bounded(raw: &str) {
    for line in raw.split("\r\n") {
        assert!(line.len() <= 998, "{} octets: {line}", line.len());
    }
}

#[test]
fn writer_renderer_long_addresses_and_references_round_trip() {
    let mut request = request();
    request.to = (0..100)
        .map(|i| {
            address(
                Some(format!("Recipient {i}")),
                format!("person{i}@example.com"),
            )
        })
        .collect();
    request.cc = request.to.clone();
    request.reply_to = request.to.clone();
    request.references = (0..100)
        .map(|i| format!("<message{i}@example.com>"))
        .collect();
    request.in_reply_to = Some(request.references.join(" "));
    let raw = render(&request).expect("render");
    bounded(&raw);
    let headers = HeaderMap::parse(raw.split("\r\n\r\n").next().expect("headers").as_bytes());
    assert_eq!(pairs(&headers.addresses("To")), pairs(&request.to));
    assert_eq!(pairs(&headers.addresses("Cc")), pairs(&request.cc));
    assert_eq!(
        pairs(&headers.addresses("Reply-To")),
        pairs(&request.reply_to)
    );
    assert_eq!(headers.message_ids("References").len(), 100);
    assert_eq!(headers.message_ids("In-Reply-To").len(), 100);
}

#[test]
fn writer_renderer_long_display_name_round_trip() {
    let mut request = request();
    request.to[0].name = Some("Møte på fjellet ".repeat(100));
    let raw = render(&request).expect("render");
    bounded(&raw);
    let headers = HeaderMap::parse(raw.split("\r\n\r\n").next().expect("headers").as_bytes());
    assert_eq!(
        headers.addresses("To")[0].name.as_deref(),
        request.to[0].name.as_deref().map(str::trim)
    );
}

#[test]
fn writer_renderer_filename_uses_rfc2231_continuations() {
    let mut request = request();
    let filename = "Møte-\u{2603}-".repeat(100) + ".txt";
    request.attachments_inline = vec![AttachmentInline {
        filename: filename.clone(),
        mime: "text/plain".into(),
        data: bytes::Bytes::from_static(b"payload"),
        inline: false,
        content_id: None,
    }];
    let raw = render(&request).expect("render");
    bounded(&raw);
    assert!(raw.contains("filename*0*=utf-8''"));
    let unfolded = raw.replace("\r\n ", " ");
    let content_type = unfolded
        .lines()
        .find(|line| line.starts_with("Content-Type: text/plain; name"))
        .expect("attachment type");
    assert_eq!(
        parse_content_type(content_type.trim_start_matches("Content-Type: ")).param("name"),
        Some(filename.as_str())
    );
    let disposition = unfolded
        .lines()
        .find(|line| line.starts_with("Content-Disposition:"))
        .expect("disposition");
    let (_, params) =
        crate::parse_disposition(disposition.trim_start_matches("Content-Disposition: "));
    assert_eq!(params, vec![("filename".into(), filename)]);
}

#[test]
fn writer_renderer_refuses_indivisible_structured_tokens() {
    let mut request = request();
    request.to[0].address = format!("{}@example.com", "a".repeat(1000));
    let error = render(&request).expect_err("oversized addr-spec");
    assert_eq!(
        *error.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    request.to[0].address = "you@example.com".into();
    request.references = vec![format!("<{}@example.com>", "a".repeat(1000))];
    let error = render(&request).expect_err("oversized msg-id");
    assert_eq!(
        *error.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
}

#[test]
fn writer_text_encoding_and_structured_token_limits() {
    let text = "a".repeat(2000);
    let header = write_header("Subject", HeaderValue::Unstructured(&text)).expect("encoded ASCII");
    bounded(&header);
    assert_eq!(
        crate::decode_encoded_words(
            header
                .replace("\r\n ", " ")
                .trim_start_matches("Subject:")
                .trim()
                .as_bytes()
        ),
        text
    );
    let text = "quoted \"text with  double spaces\"";
    assert_eq!(
        write_header("Subject", HeaderValue::Unstructured(text)).expect("text"),
        format!("Subject: {text}")
    );
    let addr = address(Some("A, ".repeat(600)), "\"local part\"@example.com".into());
    let header = write_header("To", HeaderValue::Addresses(std::slice::from_ref(&addr)))
        .expect("long ASCII phrase");
    bounded(&header);
    let parsed = HeaderMap::parse(header.as_bytes()).addresses("To");
    assert_eq!(parsed[0].address, addr.address);
    assert_eq!(
        parsed[0].name.as_deref(),
        addr.name.as_deref().map(str::trim)
    );
    let accepted = "a".repeat(997);
    bounded(
        &write_header("References", HeaderValue::MessageIds(&accepted))
            .expect("998 with folding space"),
    );
    assert!(write_header("References", HeaderValue::MessageIds(&"a".repeat(998))).is_err());
    assert!(write_header("Invalid\r\nName", HeaderValue::Unstructured("value")).is_err());
}

/// Control octets never reach the wire raw: text and phrases encode them,
/// structured values refuse them.
#[test]
fn writer_control_octets_are_encoded_or_refused() {
    let subject = write_header("Subject", HeaderValue::Unstructured("a\u{0}b")).expect("text");
    assert!(!subject.bytes().any(|b| b == 0), "{subject:?}");
    assert_eq!(
        crate::decode_encoded_words(subject.trim_start_matches("Subject:").trim().as_bytes()),
        "a\u{0}b"
    );
    let named = address(Some("Bell\u{7}".into()), "a@example.com".into());
    let to = write_header("To", HeaderValue::Addresses(std::slice::from_ref(&named))).expect("to");
    assert!(!to.bytes().any(|b| b == 7), "{to:?}");
    assert!(write_header("References", HeaderValue::MessageIds("<a\u{1}@b>")).is_err());
    assert!(write_header("Content-Type", HeaderValue::Structured("text/plain\u{7f}")).is_err());
    assert!(write_header("Content-Type", HeaderValue::Structured("text/pl\u{e9}in")).is_err());
}

/// Raw keeps legal caller bytes exactly; otherwise only words holding
/// non-ASCII or controls are encoded, and an oversize ASCII token is
/// refused rather than encoded.
#[test]
fn writer_raw_and_pre_encoded_policies() {
    let spelled = "  Fri, 21 Nov 1997 (a  comment)\r\n\t(continued)  ";
    assert_eq!(
        write_header("Date", HeaderValue::Raw(spelled)).expect("legal wire text"),
        format!("Date: {spelled}")
    );
    let mixed =
        write_header("To", HeaderValue::Raw("Se\u{e1}n <sean@example.com>")).expect("mixed");
    assert_eq!(mixed, "To: =?UTF-8?B?U2XDoW4=?= <sean@example.com>");
    assert!(write_header("X", HeaderValue::Raw(&"t".repeat(1000))).is_err());
    assert!(
        write_header(
            "X",
            HeaderValue::Raw(&format!("\u{e9} {}", "t".repeat(1000)))
        )
        .is_err()
    );
    assert_eq!(
        write_header("X", HeaderValue::PreEncoded("a\r\n b")).expect("folded"),
        "X: a\r\n b"
    );
    for bad in ["a\r\nb", "a\nb", "a\r\n \r\n b", "\u{e9}"] {
        assert!(
            write_header("X", HeaderValue::PreEncoded(bad)).is_err(),
            "{bad:?}"
        );
    }
    assert!(write_header("X", HeaderValue::PreEncoded(&"t".repeat(996))).is_err());
}
