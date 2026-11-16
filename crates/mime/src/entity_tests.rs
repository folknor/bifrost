use super::*;

struct Node {
    headers: &'static str,
    body: NodeBody,
}

enum NodeBody {
    Leaf(Vec<u8>),
    Multi(String, Vec<Node>),
}

impl MimeEntity for Node {
    fn write_header_fields(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.headers.as_bytes());
    }

    fn body(&self) -> EntityBody<'_, Self> {
        match &self.body {
            NodeBody::Leaf(bytes) => EntityBody::Encoded(bytes),
            NodeBody::Multi(boundary, children) => EntityBody::Multipart {
                boundary: boundary.into(),
                children,
            },
        }
    }
}

fn leaf(payload: &[u8]) -> Node {
    Node {
        headers: "Content-Type: text/plain\r\n",
        body: NodeBody::Leaf(payload.to_vec()),
    }
}

/// The CRLF before a delimiter is the parent's; a child payload's own final
/// CRLF is content and survives, and a closing delimiter carries no CRLF so
/// nesting adds no blank line.
#[test]
fn framing_keeps_payload_crlf_and_nests_without_blank_lines() {
    let inner = Node {
        headers: "Content-Type: multipart/alternative; boundary=\"in\"\r\n",
        body: NodeBody::Multi("in".to_owned(), vec![leaf(b"a\r\n")]),
    };
    let outer = Node {
        headers: "Content-Type: multipart/mixed; boundary=\"out\"\r\n",
        body: NodeBody::Multi("out".to_owned(), vec![inner, leaf(b"b")]),
    };
    let mut out = Vec::new();
    write_entity(&outer, &mut out);
    assert_eq!(
        String::from_utf8(out).unwrap(),
        concat!(
            "Content-Type: multipart/mixed; boundary=\"out\"\r\n\r\n",
            "--out\r\n",
            "Content-Type: multipart/alternative; boundary=\"in\"\r\n\r\n",
            "--in\r\n",
            "Content-Type: text/plain\r\n\r\n",
            "a\r\n\r\n",
            "--in--\r\n",
            "--out\r\n",
            "Content-Type: text/plain\r\n\r\n",
            "b\r\n",
            "--out--"
        )
    );
}

#[test]
fn delimiter_scan_sees_nested_delimiters_and_prefixes() {
    let nested = Node {
        headers: "Content-Type: multipart/mixed; boundary=\"abc-inner\"\r\n",
        body: NodeBody::Multi("abc-inner".to_owned(), vec![leaf(b"x")]),
    };
    assert!(contains_delimiter(&nested, "abc"));
    assert!(!contains_delimiter(&nested, "zzz"));
    assert!(contains_delimiter(&leaf(b"--zzz at the start"), "zzz"));
    assert!(!contains_delimiter(&leaf(b"mid --zzz line"), "zzz"));
}

#[test]
fn encode_payload_never_rewrites_line_endings() {
    let input = b"a\nb\r\nc\r";
    for encoding in [
        OutboundEncoding::SevenBit,
        OutboundEncoding::EightBit,
        OutboundEncoding::Binary,
    ] {
        assert_eq!(encode_payload(input, encoding), input);
    }
    let qp = encode_payload(input, OutboundEncoding::QuotedPrintable);
    let mut defects = Vec::new();
    assert_eq!(
        crate::transfer::decode_transfer(
            crate::TransferEncoding::QuotedPrintable,
            &qp,
            &mut defects
        ),
        input
    );
    let b64 = encode_payload(input, OutboundEncoding::Base64);
    assert_eq!(
        crate::transfer::decode_transfer(crate::TransferEncoding::Base64, &b64, &mut defects),
        input
    );
}

#[test]
fn chooser_keeps_text_and_bytes_apart() {
    let text = "caf\u{e9} au lait";
    assert_eq!(
        choose_encoding(EncodingInput::Text(text), false),
        OutboundEncoding::QuotedPrintable
    );
    assert_eq!(
        choose_encoding(EncodingInput::Bytes(text.as_bytes()), false),
        OutboundEncoding::Base64
    );
    assert_eq!(
        choose_encoding(EncodingInput::Text("plain"), false),
        OutboundEncoding::SevenBit
    );
}

#[test]
fn text_canonicalization_only_adds_cr_before_bare_lf() {
    let mut bytes = b"\nfirst\nsecond\r\nthird\rfourth\n".to_vec();
    canonicalize_text_crlf(&mut bytes);
    assert_eq!(bytes, b"\r\nfirst\r\nsecond\r\nthird\rfourth\r\n");
}

#[test]
fn minted_boundaries_are_unique_and_legal() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1000 {
        let boundary = mint_boundary();
        assert_eq!(boundary.len(), 40);
        assert!(boundary.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(seen.insert(boundary));
    }
}
