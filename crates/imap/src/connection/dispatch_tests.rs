#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::response::{NamespaceDescriptor, StatusKind, TaggedResponse};

// ---------------------------------------------------------------------------
// Helper  -  tagged OK with no response code
// ---------------------------------------------------------------------------

fn tagged_ok() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Ok,
        code: None,
        text: "Completed".into(),
    }
}

fn tagged_no() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::No,
        code: None,
        text: "Rejected".into(),
    }
}

fn tagged_bad() -> TaggedResponse {
    TaggedResponse {
        tag: "A001".into(),
        status: StatusKind::Bad,
        code: None,
        text: "Bad command".into(),
    }
}

fn default_ctx() -> ConsumerContext<'static> {
    ConsumerContext {
        capabilities: &[],
        enabled: &[],
        command_target: None,
        command_tag: "A001",
    }
}

#[tokio::test]
async fn streaming_fetch_consumer_does_not_drop_slow_receiver_backlog() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut consumer = StreamingFetchConsumer::new(tx);
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    for seq in 1..=300 {
        consumer.on_response(
            UntaggedResponse::Fetch(Box::new(FetchResponse {
                seq,
                uid: Some(seq + 10_000),
                ..Default::default()
            })),
            notify,
            &ctx,
        );
    }

    let finalized = Box::new(consumer).finalize(tagged_ok(), &ctx).unwrap();
    assert!(finalized.reclassified_as_events.is_empty());

    let mut received = Vec::new();
    while let Some(fetch) = rx.recv().await {
        received.push(fetch.unwrap());
    }

    assert_eq!(received.len(), 300);
    assert_eq!(received.first().map(|fetch| fetch.seq), Some(1));
    assert_eq!(received.last().map(|fetch| fetch.seq), Some(300));
}

/// Gmail labels are heap strings the server controls, so they must be part of
/// the byte estimate that drives `uid_fetch_limited` and the buffered-fetch
/// warning. A labels-only FETCH would otherwise report the flat overhead.
#[test]
fn fetch_byte_estimate_counts_gmail_labels() {
    let bare = FetchResponse {
        seq: 1,
        ..Default::default()
    };
    let labelled = FetchResponse {
        seq: 1,
        gmail_labels: Some(vec!["x".repeat(4096), "y".repeat(2048)]),
        ..Default::default()
    };
    assert_eq!(
        fetch::estimate_fetch_response_bytes(&labelled),
        fetch::estimate_fetch_response_bytes(&bare) + 4096 + 2048
    );
}

#[test]
fn fetch_consumer_propagates_terminal_no_after_data() {
    let mut consumer = FetchConsumer::new();
    let ctx = default_ctx();

    consumer.on_response(
        UntaggedResponse::Fetch(Box::new(FetchResponse {
            seq: 1,
            uid: Some(1001),
            ..Default::default()
        })),
        NotifyFlags::default(),
        &ctx,
    );

    let err = match Box::new(consumer).finalize(tagged_no(), &ctx) {
        Err(err) => err,
        Ok(_) => panic!("terminal NO must fail FETCH"),
    };
    assert!(matches!(err, Error::No { text, .. } if text == "Rejected"));
}

#[tokio::test]
async fn streaming_fetch_consumer_propagates_terminal_no_and_closes_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut consumer = StreamingFetchConsumer::new(tx);
    let ctx = default_ctx();

    consumer.on_response(
        UntaggedResponse::Fetch(Box::new(FetchResponse {
            seq: 1,
            uid: Some(1001),
            ..Default::default()
        })),
        NotifyFlags::default(),
        &ctx,
    );

    let err = match Box::new(consumer).finalize(tagged_no(), &ctx) {
        Err(err) => err,
        Ok(_) => panic!("terminal NO must fail streaming FETCH"),
    };
    assert!(matches!(err, Error::No { text, .. } if text == "Rejected"));

    let first = rx.recv().await.expect("first FETCH should be delivered");
    assert_eq!(first.unwrap().seq, 1);
    assert!(rx.recv().await.is_none());
}

#[test]
fn expunge_consumer_propagates_terminal_bad_after_data() {
    let mut consumer = ExpungeConsumer::new();
    let ctx = default_ctx();

    consumer.on_response(UntaggedResponse::Expunge(3), NotifyFlags::default(), &ctx);

    let err = match Box::new(consumer).finalize(tagged_bad(), &ctx) {
        Err(err) => err,
        Ok(_) => panic!("terminal BAD must fail EXPUNGE"),
    };
    assert!(matches!(err, Error::Bad { text, .. } if text == "Bad command"));
}

// ---------------------------------------------------------------------------
// SortConsumer  -  empty result tolerance (RFC 5256 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn sort_consumer_empty_result_returns_ok() {
    // When the server sends only a tagged OK with no `* SORT` untagged
    // response (permitted by RFC 5256 when no messages match),
    // `SortConsumer::finalize` should return an empty SearchResult.
    let consumer = Box::new(SortConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx()).unwrap();
    assert!(result.output.ids.is_empty());
    assert_eq!(result.output.mod_seq, None);
    assert!(!result.output.truncated);
    assert!(result.reclassified_as_events.is_empty());
}

// ---------------------------------------------------------------------------
// ThreadConsumer  -  empty result tolerance (RFC 5256 Section 4)
// ---------------------------------------------------------------------------

#[test]
fn thread_consumer_empty_result_returns_ok() {
    // Same as SortConsumer  -  when no messages match the THREAD criteria
    // the server may omit the untagged THREAD response entirely.
    let consumer = Box::new(ThreadConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx()).unwrap();
    assert!(result.output.is_empty());
    assert!(result.reclassified_as_events.is_empty());
}

// ---------------------------------------------------------------------------
// IdConsumer  -  solicited response NOT leaked as event
// ---------------------------------------------------------------------------

#[test]
fn id_consumer_solicited_response_not_leaked() {
    let mut consumer = IdConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* ID` response.
    consumer.on_response(
        UntaggedResponse::Id(vec![
            ("name".into(), Some("Dovecot".into())),
            ("version".into(), Some("2.3".into())),
        ]),
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx).unwrap();

    // The ID pairs must be captured.
    assert_eq!(result.output.len(), 2);
    assert_eq!(result.output[0].0, "name");
    assert_eq!(result.output[0].1.as_deref(), Some("Dovecot"));
    assert_eq!(result.output[1].0, "version");
    assert_eq!(result.output[1].1.as_deref(), Some("2.3"));

    // The solicited `* ID` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited ID response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// NamespaceConsumer  -  solicited response NOT leaked as event
// ---------------------------------------------------------------------------

#[test]
fn namespace_consumer_solicited_response_not_leaked() {
    let mut consumer = NamespaceConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* NAMESPACE` response.
    consumer.on_response(
        UntaggedResponse::Namespace {
            personal: vec![NamespaceDescriptor {
                prefix: String::new(),
                delimiter: Some('/'),
                extensions: Vec::new(),
            }],
            other: Vec::new(),
            shared: Vec::new(),
        },
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx).unwrap();

    // The namespace data must be captured.
    assert_eq!(result.output.personal.len(), 1);
    assert_eq!(result.output.personal[0].prefix, "");
    assert_eq!(result.output.personal[0].delimiter, Some('/'));
    assert!(result.output.other.is_empty());
    assert!(result.output.shared.is_empty());

    // The solicited `* NAMESPACE` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited NAMESPACE response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// EnableConsumer  -  normal capture (RFC 5161 Section 3)
// ---------------------------------------------------------------------------

#[test]
fn enable_consumer_captures_extensions() {
    let mut consumer = EnableConsumer::default();
    let ctx = default_ctx();
    let notify = NotifyFlags::default();

    // Feed a solicited `* ENABLED CONDSTORE QRESYNC` response.
    consumer.on_response(
        UntaggedResponse::Enabled(vec!["CONDSTORE".into(), "QRESYNC".into()]),
        notify,
        &ctx,
    );

    let result = Box::new(consumer).finalize(tagged_ok(), &ctx).unwrap();

    assert_eq!(result.output, vec!["CONDSTORE", "QRESYNC"]);

    // The solicited `* ENABLED` must NOT leak into reclassified_as_events.
    assert!(
        result.reclassified_as_events.is_empty(),
        "solicited ENABLED response must not be reclassified as event"
    );
}

// ---------------------------------------------------------------------------
// EnableConsumer  -  empty/missing ENABLED (Postel's law tolerance)
// ---------------------------------------------------------------------------

#[test]
fn enable_consumer_missing_enabled_returns_empty() {
    // When the server omits the ENABLED response (non-conformant per
    // RFC 5161 Section 3.2), the consumer tolerates it and returns empty.
    let consumer = Box::new(EnableConsumer::default());
    let result = consumer.finalize(tagged_ok(), &default_ctx()).unwrap();
    assert!(result.output.is_empty());
    assert!(result.reclassified_as_events.is_empty());
}
