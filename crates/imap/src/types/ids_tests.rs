#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn uid_rejects_zero() {
    assert!(Uid::new(0).is_none());
    assert_eq!(Uid::new(42).unwrap().get(), 42);
}

#[test]
fn uid_set_coalesces_numbers() {
    let set = UidSet::from_uids([
        Uid::new(5).unwrap(),
        Uid::new(1).unwrap(),
        Uid::new(2).unwrap(),
        Uid::new(4).unwrap(),
    ])
    .unwrap();
    assert_eq!(set.to_string(), "1:2,4:5");
}

#[test]
fn seq_set_keeps_sequence_semantics_separate() {
    let set = SeqSet::range(Seq::new(3).unwrap(), Seq::new(8).unwrap());
    assert_eq!(set.to_string(), "3:8");
}
