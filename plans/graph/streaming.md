# Graph streaming notes

Open questions and constraints to pin down when bifrost-graph grows
streaming surfaces under the sync-engine contract.

## Throttling

Graph returns `429 Too Many Requests` with `Retry-After`. The
transport layer (`bifrost-net`) honors the header internally with a
bounded retry budget. Past the budget, surface as a typed error
rather than looping forever. The existing client already encounters
this; move the policy from the per-call site into the transport.

## Concurrency vs paging

The existing client has a global concurrency semaphore (limit 3).
Single in-flight page is the safe default under that ceiling;
consumers on higher-tier tenants can configure higher. The sync
engine's bandwidth + concurrency budgets apply on top.

## EWS streaming notifications

`StreamingSubscription` parses notification XML chunks off a long-
poll HTTP connection. Bring in `quick-xml` for incremental parsing
rather than extending the existing EWS code. The crate already
encodes the necessary event types in `crates/graph/src/types.rs`;
the parser is the new work. Server-side lifetime cap is 30 minutes;
the stream handles reconnect internally.

## Attachment $value

Graph's `$value` byte-stream endpoint works only on file
attachments, not on inline content or item attachments (which have
their own representations). `BlobHandle::capabilities` must
distinguish: `supports_range = true` for file attachments via
`$value`, `supports_range = false` otherwise. The local types in
`crates/graph/src/types.rs` already encode the attachment-kind
distinction; the streaming surfaces must respect it.
