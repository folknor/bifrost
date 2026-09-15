//! The push-health latch in `common.rs`, pinned across the interleaving
//! where a broadcast send finds no receivers.
//!
//! `mark_push_disconnected` / `mark_push_reconnected` move the latch and
//! THEN send. A `broadcast::Sender` with no receivers discards the send, so
//! the edge can be lost while the latch has already moved. These tests pin
//! what that costs: the lost half is always the `Disconnected` warning, the
//! surviving half is always the `Reconnected` reconcile trigger, and the
//! latch never wedges.

use bifrost_types::WatchEvent;

use crate::account::push::common::{mark_push_disconnected, mark_push_reconnected};
use crate::account::{GraphAccount, PushMode};
use crate::client::GraphClient;

/// The filed interleaving: the outage begins with nobody subscribed, a
/// consumer attaches mid-outage, and the recovery edge reaches it.
///
/// The consumer is told connectivity was restored without ever having been
/// told it was lost. That asymmetry is tolerated, and this test pins WHY it
/// is safe rather than asserting the event merely arrived: the surviving
/// half is the conservative one. `Reconnected` costs the engine a
/// whole-account `Coalesced` reconcile it did not strictly need; the lost
/// half was an advisory warning carrying no coverage obligation. What would
/// NOT be safe is the latch staying up after the recovery, so that a later
/// genuine outage could no longer raise an edge - so the tail of this test
/// drives a second full cycle through the now-subscribed consumer and
/// requires both of its halves.
#[tokio::test]
async fn a_late_subscriber_sees_a_coherent_latch_after_a_lost_disconnect() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);

    // Outage with zero receivers: the latch rises, the send is discarded.
    mark_push_disconnected(&account);
    assert!(
        account
            .push_disconnected
            .load(std::sync::atomic::Ordering::SeqCst),
        "the latch rises even though nobody was listening"
    );
    assert_eq!(
        account.push_tx.receiver_count(),
        0,
        "the interleaving under test requires the Disconnected send to have been dropped"
    );

    // The consumer attaches mid-outage, having missed the edge.
    let mut events = account.push_tx.subscribe();

    // Recovery. The latch is up, so the edge fires and this consumer gets
    // the half it can act on.
    assert!(
        mark_push_reconnected(&account),
        "a latched account owes the recovery edge even though the matching \
         Disconnected was never delivered: withholding it here would cost the \
         consumer the reconcile that covers the gap"
    );
    assert!(
        matches!(events.try_recv(), Ok(WatchEvent::Reconnected)),
        "the late subscriber receives the reconcile trigger"
    );
    assert!(
        events.try_recv().is_err(),
        "and nothing else; the Disconnected it missed is not replayed"
    );

    // The latch is back down, so the account is not wedged: a second
    // outage observed by this same consumer produces BOTH halves.
    mark_push_disconnected(&account);
    assert!(
        matches!(events.try_recv(), Ok(WatchEvent::Disconnected)),
        "a lost first edge does not swallow the next one"
    );
    assert!(mark_push_reconnected(&account));
    assert!(
        matches!(events.try_recv(), Ok(WatchEvent::Reconnected)),
        "the consumer's view from its subscribe onward is a well-formed \
         Disconnected/Reconnected pair"
    );
}

/// Raising the latch must not be made conditional on the `Disconnected`
/// send succeeding.
///
/// The tempting repair for the asymmetry above is "only move the latch when
/// the send lands". It is strictly worse, and this test is the ablation for
/// it: gate the `swap(true)` on `push_tx.send(..).is_ok()` and the outage
/// below leaves the latch DOWN, so `mark_push_reconnected` returns false and
/// the consumer that was present for the recovery gets no reconcile at all.
/// That trades a superfluous reconcile for a missed one - the latch has to
/// record the outage the account actually had, not the outage somebody
/// happened to be listening for.
#[tokio::test]
async fn the_latch_records_the_outage_not_its_delivery() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);

    mark_push_disconnected(&account);
    let mut events = account.push_tx.subscribe();

    assert!(
        mark_push_reconnected(&account),
        "the undelivered Disconnected still armed the recovery edge"
    );
    assert!(matches!(events.try_recv(), Ok(WatchEvent::Reconnected)));
}
