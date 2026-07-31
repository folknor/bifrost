use super::limits::Defect;

pub(super) fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

pub(super) fn decode_charset(charset: &str, bytes: &[u8], defects: &mut Vec<Defect>) -> String {
    let lower = charset.to_ascii_lowercase();
    if matches!(lower.as_str(), "utf-8" | "utf8" | "us-ascii" | "ascii") {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let Some(encoding) = encoding_rs::Encoding::for_label(charset.as_bytes()) else {
        defects.push(Defect::UnknownCharset);
        let (value, _) = encoding_rs::WINDOWS_1252.decode_without_bom_handling(bytes);
        return value.into_owned();
    };
    let (value, _) = encoding.decode_without_bom_handling(bytes);
    value.into_owned()
}

pub(super) fn decode_charset_opt(charset: &str, bytes: &[u8]) -> Option<String> {
    let lower = charset.to_ascii_lowercase();
    if matches!(lower.as_str(), "utf-8" | "utf8" | "us-ascii" | "ascii") {
        return Some(String::from_utf8_lossy(bytes).into_owned());
    }
    let encoding = encoding_rs::Encoding::for_label(charset.as_bytes())?;
    let (value, _) = encoding.decode_without_bom_handling(bytes);
    Some(value.into_owned())
}

pub(super) fn decode_charset_lossy(charset: &str, bytes: &[u8]) -> String {
    decode_charset_opt(charset, bytes)
        .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
}
