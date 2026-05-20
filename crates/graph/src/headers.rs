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

pub fn inject_read_receipt_header(raw: &[u8]) -> Vec<u8> {
    let raw_str = String::from_utf8_lossy(raw);
    let header_block = raw_str
        .split_once("\r\n\r\n")
        .map_or(raw_str.as_ref(), |(headers, _)| headers);

    if header_block.lines().any(|line| {
        line.to_ascii_lowercase()
            .starts_with("disposition-notification-to:")
    }) {
        return raw.to_vec();
    }

    let from_addr = header_block.lines().find_map(|line| {
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

pub fn inject_read_receipt_header_base64url(raw_base64url: &str) -> Result<String, String> {
    let raw_bytes = crate::encoding::decode_base64url_nopad(raw_base64url)?;
    let patched = inject_read_receipt_header(&raw_bytes);
    Ok(crate::encoding::encode_base64url_nopad(&patched))
}

#[cfg(test)]
mod tests {
    use super::{find_header_value_case_insensitive, inject_read_receipt_header};

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
    fn injects_read_receipt_header() {
        let raw = b"From: alice@example.com\r\nTo: bob@example.com\r\nSubject: Test\r\n\r\nBody";
        let result = inject_read_receipt_header(raw);
        let result_str = String::from_utf8(result).expect("valid utf8");
        assert!(result_str.contains("Disposition-Notification-To: <alice@example.com>"));
        assert!(result_str.contains("\r\n\r\nBody"));
    }
}
