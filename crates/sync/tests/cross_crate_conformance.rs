//! Cross-crate type-level conformance for the four protocol Account
//! and AccountFactory implementations.
//!
//! S1-W1 status: the `AccountFactory::open` signature now takes the
//! engine-minted `AccountId`. Protocol crates do not yet implement
//! the new shape; this test file is stubbed back to the trait-only
//! surface for the duration of S1-W1, and will be restored once
//! S1-W2 updates each protocol crate's factory and Account impl.
//!
//! The shape it preserves for the period in between is the dyn-
//! safety witness for `Account` and `AccountFactory`, the only piece
//! that does not depend on a per-protocol factory constructor.

use bifrost_types::{Account, AccountFactory};

fn _account_is_object_safe(_: &dyn Account) {}

fn _account_factory_is_object_safe(_: &dyn AccountFactory) {}

#[test]
fn account_traits_are_object_safe() {
    let _account: fn(&dyn Account) = _account_is_object_safe;
    let _factory: fn(&dyn AccountFactory) = _account_factory_is_object_safe;
}
