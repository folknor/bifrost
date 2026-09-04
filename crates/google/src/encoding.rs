use base64::{
    Engine, alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};

use crate::{Error, Result};

// Gmail emits unpadded base64url, but a padded value (from a proxy, or a
// future API change) should not fail the whole message/attachment decode.
// Indifferent padding accepts both at zero cost.
const URL_SAFE_INDIFFERENT: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

pub(crate) fn decode_base64url_nopad(input: &str) -> Result<Vec<u8>> {
    URL_SAFE_INDIFFERENT.decode(input).map_err(Error::base64url)
}

#[cfg(test)]
mod tests {
    use super::decode_base64url_nopad;

    #[test]
    fn decodes_base64url_without_padding() {
        let decoded = decode_base64url_nopad("SGVsbG8").expect("decode should succeed");
        assert_eq!(decoded, b"Hello");
    }

    #[test]
    fn decodes_base64url_with_padding() {
        let decoded = decode_base64url_nopad("SGVsbG8=").expect("decode should succeed");
        assert_eq!(decoded, b"Hello");
    }
}
