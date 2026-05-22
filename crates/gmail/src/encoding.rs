use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

use crate::{Error, Result};

pub(crate) fn decode_base64url_nopad(input: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(input).map_err(Error::base64url)
}

#[cfg(test)]
mod tests {
    use super::decode_base64url_nopad;

    #[test]
    fn decodes_base64url_without_padding() {
        let decoded = decode_base64url_nopad("SGVsbG8").expect("decode should succeed");
        assert_eq!(decoded, b"Hello");
    }
}
