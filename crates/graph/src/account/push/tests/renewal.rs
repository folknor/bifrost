//! Row maintenance: what is due, the recreate of a vanished row, and the
//! ownership rules that keep a shared row's renewal single and its outcome
//! owned by the store rather than by whichever account started it.

use bifrost_types::SubscriptionHandle;

use crate::account::GraphAccount;
use crate::account::push::registry::DueRow;
use crate::account::push::renewal::{RowOutcome, maintain_row};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{
    created, deleted, lease, lease_expiring, reopened, row_ids, settle, webhook_account,
};

const RESOURCE: &str = "/me/mailFolders/inbox/messages";

fn not_found() -> ScriptedRestResponse {
    ScriptedRestResponse::json(
        reqwest::StatusCode::NOT_FOUND,
        serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
    )
}

fn due(account: &GraphAccount) -> Vec<DueRow> {
    account
        .webhooks
        .store()
        .renewable(account.webhooks.instance(), |row| {
            crate::webhooks::is_expiring_soon(
                &row.expires_at,
                crate::account::push::renewal::RENEWAL_THRESHOLD_MINUTES,
            )
        })
}

/// Only rows this account holds a live lease on, healthy and inside the
/// threshold, are due. A condemned lease asked for deletion, so renewing for
/// it would extend exactly what the caller wanted gone; a terminated row
/// waits for a subscribe to recover it or a release to delete it.
#[test]
fn due_rows_skip_condemned_leases_terminated_rows_and_live_expiry() {
    let account = webhook_account(&GraphClient::new("token"));
    lease_expiring(&account, "live", "/me/events", "live-sub");
    lease_expiring(&account, "condemned", "/me/contacts", "condemned-sub");
    lease_expiring(&account, "dead", "/me/messages", "dead-sub");
    lease(&account, "fresh", "/me/todo", "fresh-sub");
    account.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut("/me/contacts")
            .expect("row")
            .leases
            .values_mut()
            .for_each(|lease| lease.condemned = true);
        rows.get_mut("/me/messages").expect("row").terminated = true;
    });

    let rows = due(&account);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].resource, "/me/events");
    assert_eq!(rows[0].server_id, "live-sub");
    assert!(account.webhooks.has_live());
    assert!(account.webhooks.has_terminated());

    // Another account's lease on the same store is not this account's work.
    let other = reopened(&account, &GraphClient::new("token"));
    assert!(due(&other).is_empty());
}

/// A row recreated by a renewal step takes the vanished one's place in the
/// store rather than piling up beside it, and every live holder is owed a
/// gap for the window Graph will not replay.
#[tokio::test]
async fn a_recreated_row_replaces_the_vanished_one_in_place() {
    let client = GraphClient::new("token");
    client.script_rest([not_found(), created("fresh")]);
    let account = webhook_account(&client);
    lease_expiring(&account, "h", RESOURCE, "gone");
    lease(&account, "other", "/me/events", "healthy");

    let snapshot = due(&account).remove(0);
    assert!(matches!(
        maintain_row(&account, snapshot).await,
        RowOutcome::Settled
    ));
    assert_eq!(
        row_ids(&account),
        [
            ("/me/events".to_string(), "healthy".to_string()),
            (RESOURCE.to_string(), "fresh".to_string()),
        ]
    );
    assert_eq!(account.webhooks.events_for_tests(), 1);
}

/// The step runs in a task the account owns. A session aborted after the
/// create returned and before the store recorded it must still install the
/// replacement.
///
/// Fails if `maintain_row` awaits the step inline instead of spawning it.
#[tokio::test]
async fn an_aborted_recreate_still_installs_what_it_created() {
    let client = GraphClient::new("token");
    client.script_rest([not_found(), created("fresh")]);
    let account = webhook_account(&client);
    lease_expiring(&account, "h", RESOURCE, "stale");
    let snapshot = due(&account).remove(0);

    let blocked = account.webhooks.store().commit_gate.write().await;
    let owner = account.clone();
    let caller = tokio::spawn(async move { maintain_row(&owner, snapshot).await });
    assert!(settle(|| client.wire_attempts() == 2).await, "create done");
    caller.abort();
    let _ = caller.await;
    drop(blocked);

    assert!(
        settle(|| row_ids(&account) == [(RESOURCE.to_string(), "fresh".to_string())]).await,
        "the replacement takes the stale row's place"
    );
    assert_eq!(client.wire_attempts(), 2, "nothing was deleted");
}

/// Every holder left while the create was in flight: installing would keep
/// notifications flowing for coverage nobody wants, so the fresh row is
/// deleted again, even though the session that asked for it is gone.
#[tokio::test]
async fn a_recreate_for_a_row_nobody_holds_any_more_deletes_what_it_created() {
    let client = GraphClient::new("token");
    client.script_rest([not_found(), created("fresh"), deleted()]);
    let account = webhook_account(&client);
    lease_expiring(&account, "h", RESOURCE, "stale");
    let snapshot = due(&account).remove(0);

    let blocked = account.webhooks.store().commit_gate.write().await;
    let owner = account.clone();
    let caller = tokio::spawn(async move { maintain_row(&owner, snapshot).await });
    assert!(settle(|| client.wire_attempts() == 2).await, "create done");
    caller.abort();
    let _ = caller.await;
    account.webhooks.store().edit_for_tests(|rows| {
        rows.get_mut(RESOURCE)
            .expect("row")
            .leases
            .values_mut()
            .for_each(|lease| lease.condemned = true);
    });
    drop(blocked);

    assert!(
        settle(|| client.wire_attempts() == 3).await,
        "the unwanted replacement is deleted"
    );
    let requests = client.take_rest_requests();
    let last = requests.last().expect("recorded");
    assert_eq!(last.method.as_str(), "DELETE");
    assert!(last.url.ends_with("/subscriptions/fresh"));
    assert_eq!(
        row_ids(&account),
        [(RESOURCE.to_string(), "stale".to_string())],
        "the stale row stays for the condemned lease's retry"
    );
}

/// A recreate's admission is current demand, not the initiating account's
/// shutdown: begun by the old account of a reopen that then closes, it still
/// installs for the replacement that holds the row, and the replacement is
/// owed the gap.
#[tokio::test]
async fn a_recreate_begun_by_a_closing_account_still_serves_the_other_holder() {
    let client = GraphClient::new("token");
    client.script_rest([not_found(), created("fresh")]);
    let old = webhook_account(&client);
    lease_expiring(&old, "old", RESOURCE, "stale");
    let new = reopened(&old, &client);
    lease_expiring(&new, "new", RESOURCE, "stale");
    let snapshot = due(&old).remove(0);

    let blocked = old.webhooks.store().commit_gate.write().await;
    let owner = old.clone();
    let caller = tokio::spawn(async move { maintain_row(&owner, snapshot).await });
    assert!(settle(|| client.wire_attempts() == 2).await, "create done");
    old.shutdown.cancel();
    drop(blocked);
    let _ = caller.await;

    assert_eq!(row_ids(&new), [(RESOURCE.to_string(), "fresh".to_string())]);
    assert_eq!(
        new.webhooks.events_for_tests(),
        1,
        "the gap reached the holder"
    );
    assert_eq!(client.wire_attempts(), 2, "nothing was undone");
}

/// Two accounts of a reopen both see the shared row as due. The first step
/// renews it; the second, under the same exchange, re-reads the row, finds
/// it no longer due, and sends nothing. No second PATCH, so no stale answer
/// to reconcile.
#[tokio::test]
async fn two_accounts_renew_a_shared_row_once() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"id":"shared","expirationDateTime":"2099-01-01T00:00:00Z"}),
    )]);
    let old = webhook_account(&client);
    lease_expiring(&old, "old", RESOURCE, "shared");
    let new = reopened(&old, &client);
    lease_expiring(&new, "new", RESOURCE, "shared");
    let old_snapshot = due(&old).remove(0);
    let new_snapshot = due(&new).remove(0);

    assert!(matches!(
        maintain_row(&old, old_snapshot).await,
        RowOutcome::Settled
    ));
    assert!(matches!(
        maintain_row(&new, new_snapshot).await,
        RowOutcome::Settled
    ));
    assert_eq!(client.wire_attempts(), 1);
    assert!(due(&new).is_empty());
}

/// A terminal verdict is owed to every holder, each attributed to its own
/// scopes at delivery, and a verdict the row has since recovered from is not
/// delivered at all.
#[tokio::test]
async fn a_terminal_verdict_reaches_every_holder_until_the_row_recovers() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
    )]);
    let old = webhook_account(&client);
    lease_expiring(&old, "old", RESOURCE, "shared");
    let new = reopened(&old, &client);
    lease_expiring(&new, "new", RESOURCE, "shared");
    let snapshot = due(&old).remove(0);
    assert!(matches!(
        maintain_row(&old, snapshot).await,
        RowOutcome::Settled
    ));
    assert_eq!(old.webhooks.events_for_tests(), 1);
    assert_eq!(new.webhooks.events_for_tests(), 1);
    assert!(new.webhooks.has_terminated());

    // The row recovers (a later subscribe's PATCH) before `new` drains.
    old.webhooks.store().edit_for_tests(|rows| {
        let row = rows.get_mut(RESOURCE).expect("row");
        row.terminated = false;
        row.generation += 1_000;
    });
    let handle = SubscriptionHandle("new".to_string());
    assert!(!new.webhooks.store().leased_by(&handle).is_empty());
    match new.webhooks.pop_event() {
        Some(crate::account::push::registry::InstanceEvent::Terminal {
            resource,
            generation,
            ..
        }) => assert!(
            new.webhooks
                .store()
                .terminal_scopes(new.webhooks.instance(), &resource, generation)
                .is_none(),
            "an obsolete verdict is dropped"
        ),
        other => panic!("expected the queued verdict, got {other:?}"),
    }
}
