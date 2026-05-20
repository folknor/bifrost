use crate::Result;

pub fn find_header_value_case_insensitive<T, FName, FValue>(
    headers: &[T],
    name: &str,
    header_name: FName,
    header_value: FValue,
) -> Option<String>
where
    FName: Fn(&T) -> &str,
    FValue: Fn(&T) -> &str,
{
    headers
        .iter()
        .find(|header| header_name(header).eq_ignore_ascii_case(name))
        .map(|header| header_value(header).to_string())
}

pub fn find_header_values_case_insensitive<'a, T, FName, FValue>(
    headers: &'a [T],
    name: &str,
    header_name: FName,
    header_value: FValue,
) -> Vec<&'a str>
where
    FName: Fn(&'a T) -> &'a str,
    FValue: Fn(&'a T) -> &'a str,
{
    headers
        .iter()
        .filter(|header| header_name(header).eq_ignore_ascii_case(name))
        .map(header_value)
        .collect()
}

pub fn unfold_header_value(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\r' || c == '\n' {
            if c == '\r' && chars.peek().is_some_and(|&ch| ch == '\n') {
                chars.next();
            }
            while chars.peek().is_some_and(|&ch| ch == ' ' || ch == '\t') {
                chars.next();
            }
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

fn header_lines_unfolded(header_block: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();

    for line in header_block.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(previous) = lines.last_mut() {
                previous.push(' ');
                previous.push_str(line.trim());
            }
        } else {
            lines.push(line.to_string());
        }
    }

    lines
}

pub fn inject_read_receipt_header(raw: &[u8]) -> Vec<u8> {
    let raw_str = String::from_utf8_lossy(raw);
    let header_block = raw_str
        .split_once("\r\n\r\n")
        .map_or(raw_str.as_ref(), |(headers, _)| headers);
    let header_lines = header_lines_unfolded(header_block);

    if header_lines.iter().any(|line| {
        line.to_ascii_lowercase()
            .starts_with("disposition-notification-to:")
    }) {
        return raw.to_vec();
    }

    let from_addr = header_lines.iter().find_map(|line| {
        if !line.to_ascii_lowercase().starts_with("from:") {
            return None;
        }
        let value = line["from:".len()..].trim();
        if let Some(start) = value.rfind('<') {
            value[start + 1..]
                .find('>')
                .map(|end| &value[start + 1..start + 1 + end])
        } else {
            Some(value)
        }
    });

    let Some(sender) = from_addr else {
        return raw.to_vec();
    };

    let separator = b"\r\n\r\n";
    let Some(pos) = raw.windows(separator.len()).position(|w| w == separator) else {
        return raw.to_vec();
    };

    let header_line = format!("Disposition-Notification-To: <{sender}>\r\n");
    let mut result = Vec::with_capacity(raw.len() + header_line.len());
    result.extend_from_slice(&raw[..pos]);
    result.extend_from_slice(b"\r\n");
    result.extend_from_slice(header_line.as_bytes());
    result.extend_from_slice(&raw[pos + 2..]);
    result
}

pub fn inject_read_receipt_header_base64url(raw_base64url: &str) -> Result<String> {
    let raw_bytes = crate::encoding::decode_base64url_nopad(raw_base64url)?;
    let patched = inject_read_receipt_header(&raw_bytes);
    Ok(crate::encoding::encode_base64url_nopad(&patched))
}

#[cfg(test)]
mod tests {
    use super::{
        find_header_value_case_insensitive, find_header_values_case_insensitive,
        inject_read_receipt_header, inject_read_receipt_header_base64url, unfold_header_value,
    };
    use crate::error::{Base64Encoding, Error};

    #[derive(Clone)]
    struct Header {
        name: &'static str,
        value: &'static str,
    }

    #[test]
    fn finds_case_insensitive_header() {
        let headers = vec![Header {
            name: "Message-ID",
            value: "<id@example.com>",
        }];
        let value =
            find_header_value_case_insensitive(&headers, "message-id", |h| h.name, |h| h.value);
        assert_eq!(value.as_deref(), Some("<id@example.com>"));
    }

    #[test]
    fn finds_all_case_insensitive_headers() {
        let headers = vec![
            Header {
                name: "Authentication-Results",
                value: "mx.example; spf=pass",
            },
            Header {
                name: "authentication-results",
                value: "relay.example; spf=fail",
            },
        ];

        let values = find_header_values_case_insensitive(
            &headers,
            "authentication-results",
            |h| h.name,
            |h| h.value,
        );
        assert_eq!(
            values,
            vec!["mx.example; spf=pass", "relay.example; spf=fail"]
        );
    }

    #[test]
    fn unfolds_header_value_continuations() {
        let value = "mx.example;\r\n\tspf=pass\r\n dkim=pass";
        assert_eq!(unfold_header_value(value), "mx.example; spf=pass dkim=pass");
    }

    #[test]
    fn injects_read_receipt_header() {
        let raw = b"From: alice@example.com\r\nTo: bob@example.com\r\nSubject: Test\r\n\r\nBody";
        let result = inject_read_receipt_header(raw);
        let result_str = String::from_utf8(result).expect("valid utf8");
        assert!(result_str.contains("Disposition-Notification-To: <alice@example.com>"));
        assert!(result_str.contains("\r\n\r\nBody"));
    }

    #[test]
    fn injects_read_receipt_header_from_folded_from() {
        let raw = b"From: Alice\r\n <alice@example.com>\r\nTo: bob@example.com\r\n\r\nBody";
        let result = inject_read_receipt_header(raw);
        let result_str = String::from_utf8(result).expect("valid utf8");
        assert!(result_str.contains("Disposition-Notification-To: <alice@example.com>"));
    }

    #[test]
    fn does_not_duplicate_folded_read_receipt_header() {
        let raw = b"From: alice@example.com\r\nDisposition-Notification-To:\r\n <alice@example.com>\r\n\r\nBody";
        let result = inject_read_receipt_header(raw);
        assert_eq!(result, raw);
    }

    #[test]
    fn base64url_injection_returns_typed_decode_error() {
        let err = inject_read_receipt_header_base64url("***").expect_err("decode should fail");
        assert!(matches!(
            err,
            Error::Base64 {
                encoding: Base64Encoding::UrlSafeNoPad,
                ..
            }
        ));
    }
}
