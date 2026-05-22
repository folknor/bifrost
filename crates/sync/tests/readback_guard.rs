//! Read-back guard tests.
//!
//! Builds a synthetic `Account` impl whose `get_stream` returns
//! predetermined flag sets per id, then asserts the guard reconciles
//! applied/skipped correctly against a `FlagOp::Add` target.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use bifrost_sync::run_readback_guard;
use bifrost_types::{
    Account, AccountCapabilities, AccountFuture, AccountStream, AttachmentHandle, Batch,
    BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Change, ChangeCursor, Container,
    ContainerId, ContainerKind, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, Error as TypesError, FlagOp,
    HydratedObject, HydratedObjectKind, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, InventoryEntry, MembershipScope, Message, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, MutationResult, MutationTarget, ObjectId, Page,
    PageBoundary, PimMethodSupport, Priority, Projection, PushCapability, QuotaInfo, QuotaSignal,
    RateLimitClass, ScopeLifecycle, SearchRequest, SendRequest, SubscriptionHandle, SyncEvent,
    ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

/// Synthetic Account that returns predetermined flags for each id on
/// `get_stream(Projection::FlagsOnly)`. All other methods are
/// unreachable in this test.
struct FlagsAccount {
    caps: AccountCapabilities,
    flag_table: std::collections::HashMap<ObjectId, HashSet<String>>,
}

fn caps() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 100,
            max_wait: std::time::Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport::default(),
        conveniences: ConvenienceShape::default(),
    }
}

impl Account for FlagsAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by readback guard tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Box::pin(stream::empty())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycle> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        Box::pin(stream::empty())
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>> {
        assert_eq!(projection, Projection::FlagsOnly);
        let table = self.flag_table.clone();
        let collected: Pin<Box<dyn futures::Future<Output = Vec<ObjectId>> + Send>> =
            Box::pin(async move { ids.collect::<Vec<_>>().await });
        let s = async move {
            let ids: Vec<ObjectId> = collected.await;
            let items: Vec<HydratedObject> = ids
                .into_iter()
                .map(|id| {
                    let flags = table.get(&id).cloned().unwrap_or_default();
                    HydratedObject {
                        id,
                        kind: HydratedObjectKind::FlagsOnly(flags),
                        blobs: Vec::new(),
                    }
                })
                .collect();
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::from_millis(1),
                bytes_in: 0,
                checkpoint: None,
            })
        };
        Box::pin(stream::once(s).chain(stream::once(async { SyncEvent::Done(None) })))
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        _op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        Box::pin(stream::empty())
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        Box::pin(stream::empty())
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        Box::pin(stream::empty())
    }

    fn close(&self) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Ok(()) })
    }

    // PIM primitives stubbed to Err(Unsupported); the read-back guard
    // only exercises `get_stream`.
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, TypesError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn search(&self, _request: SearchRequest) -> AccountFuture<Result<Page<ThreadId>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, TypesError>> {
        Box::pin(async { Err(TypesError::Unsupported) })
    }
}

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test]
async fn readback_guard_marks_applied_as_skipped() {
    // Three ids retried. The server actually applied \Seen to id `a`
    // and id `b` (so they read back with the flag), but not to `c`.
    // The guard must mark a + b as skipped, leave c as still_failed.
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    table.insert(ObjectId("c".into()), set(&["\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
    };
    let ids = vec![
        ObjectId("a".into()),
        ObjectId("b".into()),
        ObjectId("c".into()),
    ];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 2);
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_handles_remove() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&[]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Remove(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "b is already without \\Seen");
    assert_eq!(outcome.still_failed, 1, "a still has \\Seen");
}

#[tokio::test]
async fn readback_guard_handles_set() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Set(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "only a matches exact set");
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_with_no_ids_is_no_op() {
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
    };
    let outcome = run_readback_guard(&acc, vec![], &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 0);
    assert_eq!(outcome.still_failed, 0);
}

/// Cross-check: a guard run against an arc'd Account behaves the same.
#[tokio::test]
async fn readback_guard_works_through_arc() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("only".into()), set(&["\\Seen"]));
    let acc: Arc<dyn Account> = Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: table,
    });
    let outcome = run_readback_guard(
        acc.as_ref(),
        vec![ObjectId("only".into())],
        &FlagOp::Add(set(&["\\Seen"])),
    )
    .await
    .expect("guard ok");
    assert_eq!(outcome.skipped, 1);
    assert_eq!(outcome.still_failed, 0);
}
