use super::*;

#[test]
fn decode_encoded_words_decodes_base64() {
    assert_eq!(decode_encoded_words(b"=?UTF-8?B?SGVsbG8=?="), "Hello");
}

#[test]
fn q_encoding_forms_decode_through_encoded_word() {
    assert_eq!(
        decode_encoded_words(b"=?UTF-8?Q?Hello_World?="),
        "Hello World"
    );
    assert_eq!(decode_encoded_words(b"=?UTF-8?Q?caf=C3=A9?="), "café");
}
