#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::validated::{MailboxName, SequenceSet};

/// Dummy consumer for testing sub-batch grouping. Implements
/// `Consumer` with a unit `Output`  -  we only care about grouping
/// logic, not response routing.
struct DummyConsumer;

impl super::super::dispatch::Consumer for DummyConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        _resp: crate::types::response::UntaggedResponse,
        _notify: super::super::NotifyFlags,
        _ctx: &super::super::dispatch::ConsumerContext,
    ) {
    }

    fn finalize(
        self: Box<Self>,
        _tagged: crate::types::response::TaggedResponse,
        _ctx: &super::super::dispatch::ConsumerContext,
    ) -> Result<super::super::dispatch::Finalized<Self::Output>, crate::error::Error> {
        Ok(super::super::dispatch::Finalized {
            output: (),
            reclassified_as_events: Vec::new(),
        })
    }
}

fn dummy_consumer() -> Box<dyn ConsumerErased> {
    Box::new(DummyConsumer) as Box<dyn ConsumerErased>
}

// ---------------------------------------------------------------------------
// group_into_sub_batches
// ---------------------------------------------------------------------------

#[test]
fn group_unique_kinds_single_batch() {
    // All unique kinds -> single sub-batch.
    let commands = vec![Command::Noop, Command::Capability, Command::Check];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 3);
    // Original indices preserved.
    assert_eq!(batches[0][0].0, 0);
    assert_eq!(batches[0][1].0, 1);
    assert_eq!(batches[0][2].0, 2);
}

#[test]
fn group_duplicate_kinds_split_into_two_batches() {
    // Two FETCH commands -> split into two batches.
    let commands = vec![
        Command::UidFetch {
            sequence_set: SequenceSet::new("1:10").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Noop,
        Command::UidFetch {
            sequence_set: SequenceSet::new("11:20").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 2);
    // First batch: UidFetch(1:10) at idx 0, Noop at idx 1.
    assert_eq!(batches[0].len(), 2);
    assert_eq!(batches[0][0].0, 0); // original index
    assert_eq!(batches[0][1].0, 1); // original index
    // Second batch: UidFetch(11:20) at idx 2.
    assert_eq!(batches[1].len(), 1);
    assert_eq!(batches[1][0].0, 2); // original index
}

#[test]
fn group_three_same_kind_three_batches() {
    // Three STATUS commands -> three batches (one per STATUS).
    let commands = vec![
        Command::Status {
            mailbox: MailboxName::new("INBOX").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Status {
            mailbox: MailboxName::new("Sent").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Status {
            mailbox: MailboxName::new("Drafts").unwrap(),
            items: "(MESSAGES)".into(),
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0][0].0, 0);
    assert_eq!(batches[1][0].0, 1);
    assert_eq!(batches[2][0].0, 2);
}

#[test]
fn group_mixed_duplicates_preserves_indices() {
    // Mix: Fetch, Status, Fetch, Status, Noop.
    // Batch 0: Fetch(0), Status(1)
    // Batch 1: Fetch(2), Status(3), Noop(4)
    let commands = vec![
        Command::UidFetch {
            sequence_set: SequenceSet::new("1:5").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Status {
            mailbox: MailboxName::new("INBOX").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::UidFetch {
            sequence_set: SequenceSet::new("6:10").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Status {
            mailbox: MailboxName::new("Sent").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Noop,
    ];
    let consumers = vec![
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
    ];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 2);
    // Batch 0: original indices 0, 1.
    let batch0_indices: Vec<usize> = batches[0].iter().map(|(i, _, _)| *i).collect();
    assert_eq!(batch0_indices, vec![0, 1]);
    // Batch 1: original indices 2, 3, 4.
    let batch1_indices: Vec<usize> = batches[1].iter().map(|(i, _, _)| *i).collect();
    assert_eq!(batch1_indices, vec![2, 3, 4]);
}

#[test]
fn group_empty_input() {
    let batches = group_into_sub_batches(Vec::new(), Vec::new());
    // Single empty batch from initialization.
    assert_eq!(batches.len(), 1);
    assert!(batches[0].is_empty());
}

#[test]
fn group_single_command() {
    let commands = vec![Command::Noop];
    let consumers = vec![dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[0][0].0, 0);
}

#[test]
fn group_searchres_pipeline_respects_save_ordering() {
    // SEARCHRES scenario (RFC 5182): two SearchSave commands followed by a
    // Fetch referencing `$`. The Fetch must land in the same batch as the
    // *second* SearchSave so `$` reflects the most recent SAVE result.
    //
    // Without the `max_batch` tracking fix (fc6ccd0), the Fetch would have
    // been placed in batch 0 (because batch 0 had no Fetch kind yet),
    // causing `FETCH $` to observe the first SAVE result instead of the
    // second.
    let commands = vec![
        Command::SearchSave {
            criteria: "SEEN".into(),
        },
        Command::SearchSave {
            criteria: "FLAGGED".into(),
        },
        Command::Fetch {
            sequence_set: SequenceSet::new("$").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);

    // Batch 0: SearchSave("SEEN") only.
    // Batch 1: SearchSave("FLAGGED") + Fetch("$").
    assert_eq!(batches.len(), 2);

    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[0][0].0, 0); // original index 0

    assert_eq!(batches[1].len(), 2);
    assert_eq!(batches[1][0].0, 1); // original index 1
    assert_eq!(batches[1][1].0, 2); // original index 2
}
