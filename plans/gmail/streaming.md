# Gmail streaming notes

Open questions and constraints to pin down when bifrost-gmail grows
streaming surfaces under the sync-engine contract.

## Stale historyId

`historyId` older than roughly 7 days returns 404 from Gmail. The
change stream surfaces this as a typed error so the engine can fall
back to a fresh inventory + diff resync. Do not retry; the cursor is
dead and the only recovery is full resync.

## Per-message-GET concurrency

Gmail's per-user rate limit is 250 quota units/sec; a `messages.get`
costs 5 units. Conservative in-flight cap of 8 leaves headroom for
other operations. Expose as engine config; consumers on lower-tier
quotas may need to cap lower.

## Batch endpoint

Gmail's `/batch` endpoint multipart-batches GET requests for latency
wins. Worth using internally? Probably defer; revisit if the per-
message stream is too slow in production. The complexity of
multipart parsing on top of bounded buffering is non-trivial and
the latency win is bounded by Gmail's per-request quota cost anyway.

## Attachment encoding

Gmail attachments are base64url-encoded inside JSON, not raw chunked
bytes. `BlobHandle::capabilities` for Gmail must set
`supports_range = false` (true byte-range resume is not available on
the JSON-wrapped form). The full attachment downloads as one JSON
body and decodes locally. Mailing-list digests routinely hit 25 MB
on this path.

## Push lifecycle

Cloud Pub/Sub `watch` for push. Bifrost manages CRUD (`users.watch`
/ `users.stop`); ingestion is a Pub/Sub listener concern outside the
crate. Push is a `WatchEvent` wake-up; the engine calls the change
stream against the freshly-advanced `historyId` to find out what
changed.
