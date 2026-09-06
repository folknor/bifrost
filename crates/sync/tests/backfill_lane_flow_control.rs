//! End-to-end pins for the bounded backfill lane (`engine::lane`).
//!
//! Cold-start pages and live changes share one broadcast ring with no
//! producer-side backpressure of its own. Backfill now takes one lane permit per
//! published page and returns it once the consumer acknowledges that page (or
//! any later one) through the ack writer, so a slow consumer applies flow
//! control instead of losing pages to ring overflow and re-reading from the last
//! durable checkpoint.
//!
//! These drive the whole pipeline against `common::StubAccount` - attach, the
//! orchestrator's fixed-plan loop, the partition runner, the broadcast, the ack
//! writer - because every one of the properties below is a property of that
//! assembly rather than of the bound itself, which has its own unit tests.
//!
//! Every test here runs on the CURRENT-THREAD runtime with `start_paused`. That
//! is deliberate and is not a weaker choice: a current-thread runtime interleaves
//! tasks at every await, and paused time additionally advances only once every
//! task is idle - which turns "the walk stopped and stayed stopped" into a
//! deterministic observation instead of a wall-clock guess, and makes "the
//! spawned waiter has parked" a fact rather than a hope. There are no wall-clock
//! sleeps anywhere in this file; the `sleep` calls are virtual.
//!
//! None of these tests is a concurrency race test. The cross-task ordering
//! properties live in `multiplexer::tests`, on a multi-threaded runtime.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{
    BackfillConfig, CheckpointStore, ConcurrencyBudget, EngineConfig, InMemoryCheckpointStore,
    MultiplexerConfig, MultiplexerEvent, SyncEngine,
};
use bifrost_types::{
    AccountFactory, AccountId, Batch, Change, Checkpoint, CoverageDomain, CursorScope, Fingerprint,
    InventoryBatch, InventoryCompletion, InventoryCoverageReport, InventoryEntry, InventoryEvent,
    InventoryPartition, InventoryPartitioning, ObjectId, PageBoundary, ServerVersion, SyncEvent,
    WarningKind,
};

/// Page width of every backfill partition in these tests, and how many of them
/// the plan holds.
const PAGE: u32 = 2;
const PARTITIONS: u32 = 40;
/// Well under `PARTITIONS`, so a bound that is not honoured is unmissable.
const LANE: usize = 2;
/// Deliberately smaller than `PARTITIONS * PAGE`: an unbounded producer
/// overruns it and the consumer sees a lag warning plus missing ids. That is
/// what gives `no_backfill_page_is_lost_or_duplicated_under_the_bound` its bite.
const RING: usize = 8;

fn entry(id: &str) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(id.into()),
        memberships: Vec::new(),
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: ServerVersion::Unavailable,
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// Ids this partition's page carries.
///
/// The FIRST partition is deliberately fat and the rest are thin. A test that
/// observes a stall against uniform pages can pass against a producer with no
/// bound at all if the scheduling happens to line up; an asymmetric head makes
/// "how far did it get" a real measurement rather than a coincidence, and makes
/// the delivered-id set in the no-loss test asymmetric too, so an off-by-a-page
/// answer cannot look correct.
fn page_ids(from: u32) -> Vec<String> {
    let width = if from == 0 { 20 } else { PAGE };
    (0..width).map(|n| format!("p{from}-{n}")).collect()
}

fn expected_ids() -> HashSet<String> {
    (0..PARTITIONS)
        .flat_map(|partition| page_ids(partition * PAGE))
        .collect()
}

/// A stub whose fixed plan is `PARTITIONS` page windows, each yielding one
/// checkpoint-bearing batch and then `Done`.
fn paged_stub(scope: &CursorScope) -> common::StubAccount {
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(PARTITIONS * PAGE),
        page_size: Some(PAGE),
    };
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        let from = match partition {
            InventoryPartition::Page { from, .. } => *from,
            other => panic!("the PageCount plan yields page partitions only: {other:?}"),
        };
        let items: Vec<InventoryEntry> = page_ids(from).iter().map(|id| entry(id)).collect();
        vec![
            InventoryEvent::Batch(
                InventoryBatch::try_new(
                    items,
                    PageBoundary::Final,
                    Duration::ZERO,
                    0,
                    None,
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                )
                .expect("a Final page with no checkpoint is boundary-valid"),
            ),
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
            )),
        ]
    }));
    stub
}

fn config(poll: Option<Duration>) -> EngineConfig {
    let mut multiplexer = MultiplexerConfig {
        changes_capacity: RING,
        ..MultiplexerConfig::default()
    };
    if let Some(poll) = poll {
        multiplexer.poll_initial = poll;
        multiplexer.poll_min = poll;
        multiplexer.poll_max = poll;
    }
    EngineConfig {
        multiplexer,
        backfill: BackfillConfig {
            lane_capacity: LANE,
            ..BackfillConfig::default()
        },
        ..EngineConfig::default()
    }
}

async fn attach(
    account_id: &AccountId,
    stub: Arc<common::StubAccount>,
    store: Arc<InMemoryCheckpointStore>,
    config: EngineConfig,
    budget: Option<ConcurrencyBudget>,
) -> SyncEngine {
    attach_over(
        account_id,
        stub,
        store as Arc<dyn CheckpointStore>,
        config,
        budget,
    )
    .await
}

async fn attach_over(
    account_id: &AccountId,
    stub: Arc<common::StubAccount>,
    store: Arc<dyn CheckpointStore>,
    mut config: EngineConfig,
    budget: Option<ConcurrencyBudget>,
) -> SyncEngine {
    if let Some(budget) = budget {
        config.budget = budget;
    }
    let engine = SyncEngine::builder()
        .config(config)
        .checkpoints(store)
        .build()
        .expect("engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    engine
}

fn backfill_checkpoint(event: &MultiplexerEvent) -> Option<&bifrost_types::BackfillCheckpoint> {
    match &event.checkpoint {
        Some(Checkpoint::Backfill(checkpoint)) => Some(checkpoint),
        _ => None,
    }
}

fn batch_ids(event: &MultiplexerEvent) -> Vec<String> {
    match event.event.as_ref() {
        SyncEvent::Batch(batch) => batch
            .items
            .iter()
            .filter_map(|change| match change {
                Change::ObjectChange(object) => Some(object.id.0.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The partition count once the walk has demonstrably STOPPED advancing.
///
/// "Read `LANE` pages" only proves the producer got that far; a test that then
/// asserts something about a parked producer has to establish it is parked, or
/// it is asserting about a producer that was simply between partitions.
///
/// Under `start_paused` this is deterministic rather than a wall-clock guess:
/// tokio only advances virtual time once every task is idle, so a `sleep` that
/// returns is proof that nothing was still running - including the orchestrator's
/// own 1s rescan tick, which has had its chance too.
async fn settled_partition_count(stub: &Arc<common::StubAccount>) -> usize {
    loop {
        let before = stub.walked_partitions().len();
        tokio::time::sleep(Duration::from_millis(200)).await;
        if stub.walked_partitions().len() == before {
            return before;
        }
    }
}

/// Read forward until `LANE` backfill pages have arrived unacknowledged, which
/// is the state in which the producer is parked at the bound.
async fn read_to_the_bound(
    events: &mut bifrost_sync::ChangesReceiver,
) -> Vec<(CursorScope, Checkpoint, bifrost_sync::PublicationId)> {
    let mut held = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while held.len() < LANE {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "a bounded backfill lane must not lag its own consumer"
            );
            if let Some(checkpoint) = backfill_checkpoint(&event) {
                held.push((
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone().expect("checkpointed publication"),
                ));
            }
        }
    })
    .await
    .expect("the first pages must reach the consumer");
    held
}

/// THE property. A consumer that takes delivery but does not acknowledge stops
/// the producer: no further partition is even requested from the account, and no
/// further page is published, until an acknowledgement comes back.
#[tokio::test(start_paused = true)]
async fn a_slow_consumer_stalls_the_backfill_producer() {
    let account_id = AccountId("lane-stall".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let held = read_to_the_bound(&mut events).await;

    // Nothing more arrives while the acknowledgements are withheld.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "the producer must park at the bound, not keep publishing"
    );
    // And the bound is enforced BEFORE the wire call, not merely at the
    // broadcast: at most one page is in the producer's hand past the bound.
    let walked = stub.walked_partitions().len();
    assert!(
        walked <= LANE + 1,
        "a parked producer must not keep asking the account for partitions; walked {walked}"
    );

    // One acknowledgement, one page of headroom - the existing backfill ack IS
    // the permit signal.
    let (ack_scope, checkpoint, publication) = held[0].clone();
    engine
        .ack_checkpoint(&account_id, ack_scope, checkpoint, Some(publication))
        .await
        .expect("the ack persists");
    let resumed = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("an acknowledgement must release the producer")
        .expect("the broadcast stays open");
    assert!(
        backfill_checkpoint(&resumed).is_some(),
        "and what it releases is the next backfill page"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The fairness rule, stated as a test: the lane bound is a producer-side gate
/// on backfill and nothing else consults it, so live changes keep flowing at
/// full rate while cold start is parked.
#[tokio::test(start_paused = true)]
async fn live_changes_still_flow_while_the_backfill_producer_is_parked() {
    let account_id = AccountId("lane-live".to_owned());
    let scope = CursorScope::Account;
    let mut stub = paged_stub(&scope);
    // A live batch on every poll, carrying no checkpoint so it is unambiguously
    // distinguishable from a backfill page and takes no lane permit.
    stub.changes_hook = Some(Arc::new(|_cursor| {
        vec![SyncEvent::Batch(Batch {
            items: vec![Change::ObjectChange(bifrost_types::ObjectChange {
                id: ObjectId("live".into()),
                kind: bifrost_types::ObjectChangeKind::Updated,
            })],
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })]
    }));
    let stub = Arc::new(stub);
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(Some(Duration::from_millis(50))),
        // FINDING 3's bite. `per_account = 2` with the default 1/4 mutation
        // share leaves the account exactly ONE sync permit, and `global = 1`
        // makes it the whole engine's. A backfill that parks while holding it
        // starves the live lane on the very mechanism built to protect it. The
        // default budget has spare permits and cannot observe this at all.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let _held = read_to_the_bound(&mut events).await;
    let walked_at_the_bound = settled_partition_count(&stub).await;
    assert!(
        walked_at_the_bound <= LANE + 1,
        "the producer must be parked at the bound; walked {walked_at_the_bound}"
    );

    // Now, with no acknowledgement at all, live changes must keep arriving.
    let mut live = 0_usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        while live < 3 {
            let event = events.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_none() && batch_ids(&event) == vec!["live".to_owned()]
            {
                live += 1;
            }
        }
    })
    .await
    .expect("a parked backfill must not stall the live lane");

    assert_eq!(
        stub.walked_partitions().len(),
        walked_at_the_bound,
        "and the backfill producer stayed parked throughout"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The whole point of the change: with the bound in place and the consumer
/// acknowledging as it reads, every inventory page reaches the consumer exactly
/// once through a ring far smaller than the walk.
///
/// Before the lane, this walk overran a 8-slot ring, the receiver reported
/// `ChangeStreamLagged`, the account's outstanding publications were abandoned,
/// and the lost pages' ids never arrived at all.
#[tokio::test(start_paused = true)]
async fn no_backfill_page_is_lost_or_duplicated_under_the_bound() {
    let account_id = AccountId("lane-no-loss".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0_usize;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "the bound exists precisely so this walk never lags its consumer"
            );
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            pages += 1;
            seen.extend(batch_ids(&event));
            // A DELIBERATELY slow consumer. Without it the reader keeps up with
            // the producer by accident on a fast machine, and the test measures
            // nothing: the ring is 8 slots against a 40-partition walk, so an
            // unbounded producer has to be given the chance to run away before
            // "it did not" means anything.
            tokio::time::sleep(Duration::from_millis(5)).await;
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                break;
            }
        }
    })
    .await
    .expect("the walk must run to its completion sentinel");

    let unique: HashSet<String> = seen.iter().cloned().collect();
    assert_eq!(
        seen.len(),
        unique.len(),
        "no page may be delivered twice under the bound"
    );
    assert_eq!(unique, expected_ids(), "and none may be lost");
    assert_eq!(
        pages,
        PARTITIONS as usize + 1,
        "one checkpoint-bearing batch per partition, plus the completion sentinel"
    );
    assert_eq!(
        stub.walked_partitions().len(),
        PARTITIONS as usize,
        "and every partition was walked exactly once"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A retained receiver must see the channel CLOSE after detach, not wait for
/// ever on a channel it is itself keeping open.
///
/// The receiver's handle back to the account's delivery gate is weak for exactly
/// this reason: a strong one kept the broadcast sender alive, so a consumer
/// draining the ring after `detach` would sit in `recv()` indefinitely with no
/// producer left and no way to learn that.
#[tokio::test(start_paused = true)]
async fn a_retained_receiver_sees_the_channel_close_after_detach() {
    let account_id = AccountId("lane-close".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let _held = read_to_the_bound(&mut events).await;

    engine.detach(&account_id).await.expect("detach succeeds");
    drop(engine);

    // Drain whatever is still buffered, then the channel must report Closed.
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Ok(_) => {}
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("a detached account's stream must not leave its consumer waiting");
    assert!(
        matches!(closed, tokio::sync::broadcast::error::RecvError::Closed),
        "expected Closed after detach, got {closed:?}"
    );
}

/// The LIVE driver refuses a backfill checkpoint too, exactly as the fusion path
/// does.
///
/// Same structural reason in both: the driver registers the publication and then
/// sends on the raw sender, so the entry is never stamped with a delivery - and
/// an unsent stamp is deliberately never swept, leaving a `Lane::Backfill` entry
/// charged against the account's backfill bound until an acknowledgement, a lag
/// or a reset frees it. Latent - no provider here emits one - and refused because
/// refusing costs nothing and the alternative narrows every later cold start.
#[tokio::test(start_paused = true)]
async fn the_live_driver_refuses_a_backfill_checkpoint() {
    let account_id = AccountId("lane-live-backfill-cp".to_owned());
    let scope = CursorScope::Account;
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    let hook_scope = scope.clone();
    stub.changes_hook = Some(Arc::new(move |_cursor| {
        vec![SyncEvent::Batch(Batch {
            items: Vec::new(),
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: Some(Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
                scope: hook_scope.clone(),
                partition: bifrost_types::Partition(b"complete".to_vec()),
                progress_marker: None,
                progress: bifrost_types::BackfillProgress::default(),
                envelope_version: 1,
            })),
        })]
    }));
    let engine = attach(
        &account_id,
        Arc::new(stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(Some(Duration::from_millis(50))),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let terminated = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            match event.event.as_ref() {
                SyncEvent::Terminated(error) => {
                    // The MESSAGE, not merely the classification. The two contract
                    // violations share a kind and a terminal outcome, so nothing
                    // but the detail distinguishes them - and they were silently
                    // swapped for a round, sending whoever read the log after a
                    // boundary problem that did not exist.
                    let chain = format!("{:?}", error.chain());
                    assert!(
                        chain.contains("backfill checkpoint"),
                        "the refusal must say what actually happened: {chain}"
                    );
                    return;
                }
                SyncEvent::Batch(batch) => assert!(
                    batch.checkpoint.is_none(),
                    "a backfill checkpoint must never be published from the live driver: \
                     it is charged against the bound and nothing will ever sweep it"
                ),
                _ => {}
            }
        }
    })
    .await;
    assert!(
        terminated.is_ok(),
        "the scope must be terminated rather than the batch published"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// FINDING 1, end to end. The completion sentinel waits on the same bound as a
/// page, and a version of that wait which re-acquired scheduler admission on the
/// wake left the account's only sync permit held for the rest of the attachment.
///
/// The bite is the SECOND scope. With one sync permit for the whole engine, a
/// leaked admission means the second scope's barrier query and partitions can
/// never be admitted, so its walk simply never happens - silently, with the
/// first scope looking perfectly healthy.
#[tokio::test(start_paused = true)]
async fn a_completed_walk_does_not_strand_the_accounts_admission() {
    let account_id = AccountId("lane-sentinel-admission".to_owned());
    let first = CursorScope::Account;
    let second = CursorScope::Type(bifrost_types::ObjectType::Email);
    let mut stub = paged_stub(&first);
    stub.scopes = vec![first.clone(), second.clone()];
    // Two partitions per scope, so each walk reaches its sentinel quickly.
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(2 * PAGE),
        page_size: Some(PAGE),
    };
    let stub = Arc::new(stub);
    let mut config = config(None);
    // A bound of ONE forces the sentinel to PARK: it is emitted after the last
    // page of the walk, which is still unacknowledged at that moment, so the
    // sentinel's wait is a real wait rather than a fast path. Without that this
    // test never exercises the wake at all, and the leak it exists to catch
    // happens on the wake.
    config.backfill.lane_capacity = 1;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config,
        // One sync permit for the entire engine: a stranded admission is fatal
        // rather than merely wasteful.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut completed: HashSet<String> = HashSet::new();
    let both = tokio::time::timeout(Duration::from_secs(20), async {
        while completed.len() < 2 {
            let event = events.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            if checkpoint.partition.0 == b"complete" {
                completed.insert(format!("{:?}", checkpoint.scope));
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await;
    assert!(
        both.is_ok(),
        "both scopes must complete; a sentinel that kept the account's only sync \
         permit stops the second one dead. completed: {completed:?}, walked: {}",
        stub.walked_partitions().len()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// Teardown, first half. A producer parked on lane capacity must not cost
/// `detach` its whole `detach_timeout` and then be aborted mid-partition.
#[tokio::test(start_paused = true)]
async fn detach_does_not_wait_out_a_producer_parked_on_capacity() {
    let account_id = AccountId("lane-detach".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let _held = read_to_the_bound(&mut events).await;
    // Establish that the producer has actually REACHED the capacity wait rather
    // than merely being between partitions: the walk must have stopped and
    // stayed stopped. Without this the timing assertion below passes against a
    // producer that was never parked at all, which is what it is meant to prove
    // something about.
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= LANE + 1,
        "the producer must be parked at the bound, not still walking; walked {parked_at}"
    );

    let started = tokio::time::Instant::now();
    engine.detach(&account_id).await.expect("detach succeeds");
    let elapsed = started.elapsed();

    // The timeout IS the boundary between draining and aborting: `detach` awaits
    // each worker until the deadline and only then aborts it, so finishing well
    // inside the deadline is what "the parked producer was woken and drained"
    // looks like from outside. Half the deadline, so the assertion is not one
    // slow scheduling tick away from meaningless.
    assert!(
        elapsed * 2 < EngineConfig::default().detach_timeout,
        "detach took {elapsed:?} against a {:?} timeout, which means the parked producer \
         was awaited to the deadline and aborted rather than woken",
        EngineConfig::default().detach_timeout
    );
    // Corroboration that the workers really finished rather than being abandoned
    // mid-flight: teardown reached `Account::close()`, and nothing kept walking
    // afterwards.
    assert_eq!(
        stub.closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "teardown must reach the account close"
    );
    let after_detach = stub.walked_partitions().len();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stub.walked_partitions().len(),
        after_detach,
        "no worker may still be walking partitions after detach returned"
    );
}

/// FINDING 4, end to end. A subscriber that received pages and then dropped can
/// never acknowledge them - and neither can its REPLACEMENT, which joins at the
/// ring's tail and so starts behind those pages.
///
/// The bite is the resubscribe, and it is immediate: it lands well inside the
/// parked producer's one-second liveness tick, so a rule that asks "is anyone
/// subscribed" never observes a gap and releases nothing, forever. The
/// assertions are the two halves that matter - the walk resumes, AND the
/// replacement receiver actually gets the remaining pages rather than the
/// producer running dry into a channel it is not reading.
#[tokio::test(start_paused = true)]
async fn replacing_the_stream_while_the_producer_is_parked_releases_it() {
    let account_id = AccountId("lane-replace".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let _held = read_to_the_bound(&mut events).await;
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= LANE + 1,
        "the producer must be parked at the bound; walked {parked_at}"
    );

    // Drop and immediately replace, with no acknowledgement in between.
    drop(events);
    let mut replacement = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // The replacement must receive real pages, which can only happen if the
    // producer was released.
    // Exactly `LANE` pages can flow before the producer parks again: the
    // replacement acknowledges nothing, so the bound reasserts itself
    // immediately. Asking for more would be asking the bound to fail.
    let mut received = 0_usize;
    let delivered = tokio::time::timeout(Duration::from_secs(10), async {
        while received < LANE {
            let event = replacement.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some() {
                received += 1;
            }
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "a producer parked behind a consumer that has been replaced must be released and \
         deliver to the replacement; it stopped at {} of {PARTITIONS} partitions after \
         receiving {received}",
        stub.walked_partitions().len()
    );
    assert!(
        stub.walked_partitions().len() > parked_at,
        "and the walk itself must have moved on"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The bound of ONE case: forty consecutive pages, each acknowledged the instant
/// it arrives, so every page in the walk has to free its own capacity before the
/// next one can be published.
///
/// SEQUENTIAL, and described as such. It runs on the current-thread runtime under
/// paused time like the rest of this file, so there is no cross-thread race here
/// and this test never claimed to exercise one - it exercises the tightest
/// possible ack-then-publish chain, where a single page whose capacity is not
/// returned stalls the whole walk immediately rather than eventually.
#[tokio::test(start_paused = true)]
async fn an_eagerly_acknowledging_consumer_never_strands_lane_capacity() {
    let account_id = AccountId("lane-eager".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let mut config = config(None);
    config.backfill.lane_capacity = 1;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config,
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut pages = 0_usize;
    let mut seen: Vec<String> = Vec::new();
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "a bound of one cannot lag an 8-slot ring; a warning here means the \
                 producer escaped the bound rather than that the consumer was slow"
            );
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            pages += 1;
            seen.extend(batch_ids(&event));
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                break;
            }
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "a bound of one must not strand capacity against an eager acker; the walk stalled \
         after {pages} pages at {} of {PARTITIONS} partitions",
        stub.walked_partitions().len()
    );

    // A walk that merely "finished" proves little: it has to have delivered
    // everything, once each, and asked the account for each partition once.
    let unique: HashSet<String> = seen.iter().cloned().collect();
    assert_eq!(seen.len(), unique.len(), "no page delivered twice");
    assert_eq!(unique, expected_ids(), "and none lost");
    assert_eq!(pages, PARTITIONS as usize + 1);
    assert_eq!(stub.walked_partitions().len(), PARTITIONS as usize);

    engine.detach(&account_id).await.expect("detach succeeds");
}
