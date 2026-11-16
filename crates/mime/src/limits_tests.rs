use super::*;

#[test]
fn mime_limits_defaults_are_the_documented_values() {
    let limits = MimeLimits::default();
    assert_eq!(limits.max_input_bytes, 64 * 1024 * 1024);
    assert_eq!(limits.max_depth, 20);
    assert_eq!(limits.max_parts, 1000);
    assert_eq!(limits.max_header_bytes, 1024 * 1024);
    assert_eq!(limits.max_text_bytes, 4 * 1024 * 1024);
}
