use bifrost_types::IdempotencyKey;

/// Gmail messages endpoints do not document a client-mintable
/// idempotency header. Keep this helper explicit so mutation call
/// sites make the no-wire-token posture visible.
pub(crate) fn wire_idempotency_headers(
    _key: &IdempotencyKey,
) -> &'static [(&'static str, &'static str)] {
    &[]
}
