//! `SyncControl` generation-gating and boundary-compose tests.
//!
//! The pause / checkpoint_now waiter contract is "the returned
//! checkpoint has been persisted AFTER the request was made": a
//! checkpoint recorded before the request must never satisfy it (that
//! was the stale-snapshot bug the generation counter exists to
//! prevent). Nothing in the suite previously pinned this.
//!
//! The harness keeps watch receivers alive to mirror the engine worker
//! topology, while the final tests verify that canonical snapshots
//! remain correct even after every receiver is dropped.
//!
//! All tests are in-process and deterministic. The two park-proof
//! tests wrap a genuinely-unresolvable future in a short real timeout
//! (the future can never complete because no `record_checkpoint` ever
//! fires, so the timeout can only elapse - there is no race to lose).
//! `tokio::join!` sequences the record-after-request interleavings.

use std::time::Duration;

use bifrost_sync::{Boundary, BoundaryRequest, BoundaryView, SyncControl};
use bifrost_types::{
    AccountId, ChangeCursor, Checkpoint, Control, CursorScope, OpaqueChangeState, Priority,
    ProtocolKind,
};
use tokio::sync::watch;

fn sample_checkpoint(state: &[u8]) -> Checkpoint {
    Checkpoint::Change(ChangeCursor {
        scope: CursorScope::Account,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Imap,
            envelope_version: 1,
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: 1,
    })
}

struct ControlHarness {
    control: SyncControl,
    boundary: Boundary,
    // Receivers held for the test's lifetime, mirroring the engine's
    // AccountSlot / worker topology. They are no longer load-bearing
    // for the canonical value (the senders use `send_replace`), but
    // keeping them means these tests exercise the same wakeup paths
    // the engine does; the receiver-less behavior is pinned separately
    // at the bottom of this file.
    _boundary_view: BoundaryView,
    _priority_rx: watch::Receiver<Priority>,
    _cap_rx: watch::Receiver<Option<u64>>,
}

fn make_control() -> ControlHarness {
    let (boundary, boundary_view) = Boundary::new();
    let (priority_tx, priority_rx) = watch::channel(Priority::Normal);
    let (cap_tx, cap_rx) = watch::channel(None);
    let control = SyncControl::new(
        AccountId("ctl-test".into()),
        boundary.clone(),
        priority_tx,
        cap_tx,
    );
    ControlHarness {
        control,
        boundary,
        _boundary_view: boundary_view,
        _priority_rx: priority_rx,
        _cap_rx: cap_rx,
    }
}

#[tokio::test]
async fn checkpoint_recorded_before_the_request_does_not_satisfy_it() {
    let h = make_control();
    // A checkpoint lands (consumer acked some earlier batch) ...
    h.control
        .record_checkpoint(sample_checkpoint(b"stale"))
        .await;
    // ... then the consumer asks for a fresh boundary. The stale
    // snapshot must NOT be returned; with no new record arriving the
    // call parks (proven by the timeout elapsing).
    let result = tokio::time::timeout(Duration::from_millis(100), h.control.checkpoint_now()).await;
    assert!(
        result.is_err(),
        "checkpoint_now must wait for a post-request checkpoint, not return the stale one"
    );
}

#[tokio::test]
async fn pause_with_no_checkpoint_traffic_parks() {
    // Documents the liveness property as it exists today: on an idle
    // account (no batches -> no consumer acks -> no record_checkpoint)
    // `pause` does not resolve.
    // this pin is a description of current behavior, not an
    // endorsement.
    let h = make_control();
    let result = tokio::time::timeout(Duration::from_millis(100), h.control.pause()).await;
    assert!(
        result.is_err(),
        "pause() parks until a checkpoint is recorded"
    );
    // The boundary flip itself happened immediately, so workers do
    // park even while the waiter is still pending.
    assert_eq!(h.boundary.snapshot(), BoundaryRequest::Pause);
}

#[tokio::test]
async fn checkpoint_now_returns_a_checkpoint_recorded_after_the_request() {
    let h = make_control();
    let recorder = h.control.clone();
    let (result, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        // Let checkpoint_now bump its generation and park first.
        tokio::task::yield_now().await;
        recorder
            .record_checkpoint(sample_checkpoint(b"fresh"))
            .await;
    });
    let checkpoint = result.expect("checkpoint_now resolves on a fresh record");
    let Checkpoint::Change(cursor) = checkpoint else {
        panic!("expected the Change checkpoint back");
    };
    assert_eq!(cursor.server_state.bytes, b"fresh".to_vec());
    // A checkpoint_now issued while running restores Run afterwards.
    assert_eq!(h.boundary.snapshot(), BoundaryRequest::Run);
}

#[tokio::test]
async fn checkpoint_now_restores_a_pre_existing_pause() {
    // A checkpoint request made while paused flushes one boundary and
    // remains paused (the documented compose rule on BoundaryRequest).
    let h = make_control();
    h.boundary.set(BoundaryRequest::Pause);
    let recorder = h.control.clone();
    let (result, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        tokio::task::yield_now().await;
        recorder.record_checkpoint(sample_checkpoint(b"cp")).await;
    });
    assert!(result.is_ok());
    assert_eq!(
        h.boundary.snapshot(),
        BoundaryRequest::Pause,
        "checkpoint_now while paused must stay paused afterwards"
    );
}

#[tokio::test]
async fn checkpoint_now_does_not_clobber_a_concurrent_pause() {
    let h = make_control();
    let recorder = h.control.clone();
    let boundary = h.boundary.clone();
    let (result, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        tokio::task::yield_now().await;
        boundary.set(BoundaryRequest::Pause);
        recorder.record_checkpoint(sample_checkpoint(b"cp")).await;
    });
    assert!(result.is_ok());
    assert_eq!(
        h.boundary.snapshot(),
        BoundaryRequest::Pause,
        "the concurrent pause must win over checkpoint restoration"
    );
}

#[tokio::test]
async fn control_calls_never_resurrect_a_stopping_account() {
    // `detach` writes `Stop` and then waits for workers to drain. A
    // consumer racing that teardown with a pause / resume / checkpoint
    // must not un-stop them: a `Pause` written over `Stop` parks the
    // workers instead, and detach then burns its whole timeout.
    let h = make_control();
    h.boundary.set(BoundaryRequest::Stop);

    h.control.resume();
    assert_eq!(h.boundary.snapshot(), BoundaryRequest::Stop);

    let recorder = h.control.clone();
    let (result, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        tokio::task::yield_now().await;
        recorder.record_checkpoint(sample_checkpoint(b"cp")).await;
    });
    assert!(result.is_ok(), "the final flush still satisfies the waiter");
    assert_eq!(
        h.boundary.snapshot(),
        BoundaryRequest::Stop,
        "checkpoint_now must not displace a stop request"
    );
}

#[tokio::test]
async fn pause_leaves_the_boundary_paused_after_resolving() {
    let h = make_control();
    let recorder = h.control.clone();
    let (result, ()) = tokio::join!(h.control.pause(), async move {
        tokio::task::yield_now().await;
        recorder.record_checkpoint(sample_checkpoint(b"cp")).await;
    });
    assert!(result.is_ok());
    assert_eq!(h.boundary.snapshot(), BoundaryRequest::Pause);
    // resume() flips back to Run.
    h.control.resume();
    assert_eq!(h.boundary.snapshot(), BoundaryRequest::Run);
}

#[tokio::test]
async fn priority_and_bandwidth_snapshots_reflect_control_calls() {
    let h = make_control();
    assert_eq!(h.control.priority_snapshot(), Priority::Normal);
    h.control.priority(Priority::Bulk);
    assert_eq!(h.control.priority_snapshot(), Priority::Bulk);

    assert_eq!(h.control.bandwidth_cap_snapshot(), None);
    h.control.bandwidth_cap(Some(125_000));
    assert_eq!(h.control.bandwidth_cap_snapshot(), Some(125_000));

    assert_eq!(h.control.bandwidth_observed(), 0);
    h.control.observe_bandwidth(42);
    assert_eq!(h.control.bandwidth_observed(), 42);
}

#[tokio::test]
async fn two_sequential_checkpoint_requests_each_need_their_own_record() {
    // Generation gating is per-request: the record that satisfied the
    // first request does not satisfy a second request made after it.
    let h = make_control();

    let recorder = h.control.clone();
    let (first, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        tokio::task::yield_now().await;
        recorder.record_checkpoint(sample_checkpoint(b"one")).await;
    });
    assert!(first.is_ok());

    let recorder = h.control.clone();
    let (second, ()) = tokio::join!(h.control.checkpoint_now(), async move {
        tokio::task::yield_now().await;
        recorder.record_checkpoint(sample_checkpoint(b"two")).await;
    });
    let Checkpoint::Change(cursor) = second.expect("second request resolves") else {
        panic!("expected Change checkpoint");
    };
    assert_eq!(cursor.server_state.bytes, b"two".to_vec());
}

#[tokio::test]
async fn boundary_snapshot_updates_after_every_view_is_dropped() {
    let (boundary, view) = Boundary::new();
    drop(view);
    boundary.set(BoundaryRequest::Pause);
    assert_eq!(
        boundary.snapshot(),
        BoundaryRequest::Pause,
        "the sender retains the canonical boundary value"
    );
}

#[tokio::test]
async fn priority_and_cap_snapshots_update_without_receivers() {
    let (boundary, _view) = Boundary::new();
    let (priority_tx, priority_rx) = watch::channel(Priority::Normal);
    let (cap_tx, cap_rx) = watch::channel(Some(1_000_u64));
    drop(priority_rx);
    drop(cap_rx);
    let control = SyncControl::new(AccountId("ctl-lost".into()), boundary, priority_tx, cap_tx);

    control.priority(Priority::Foreground);
    assert_eq!(
        control.priority_snapshot(),
        Priority::Foreground,
        "priority snapshot must retain the latest requested value"
    );

    control.bandwidth_cap(None);
    assert_eq!(
        control.bandwidth_cap_snapshot(),
        None,
        "bandwidth-cap snapshot must retain the latest requested value"
    );
}
