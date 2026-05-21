# JMAP streaming notes

Carry-over from the cross-cutting streaming plan. Open questions to
pin down when bifrost-jmap grows streaming surfaces under the
sync-engine contract.

## Resume mid-query

JMAP `position` on `*/query` is a count, not opaque. A paging
consumer can resume from a specific offset, which is friendlier to
the sync engine's `BackfillCheckpoint` model than an opaque token.
The streaming surface should expose mid-stream resume from a count,
not hide it behind a change-cursor-only API.

## Push reconnect policy

`WebSocketPushEnable` and SSE reconnect defaults should match RFC
8887 guidance for backoff and consecutive-failure caps. Lean
configurable, with sane defaults baked in. The push surface is a
`WatchEvent` invalidation stream per the sync-engine contract;
reconnect state is engine-internal and surfaces only as
`Disconnected` / `Reconnected` events.

## Raw response escape hatch

Some consumers batch multiple methods in one HTTP request for
latency (JMAP allows arbitrary method composition per request). The
streams should not be the only API; keep a builder-style raw-
request surface alongside the streaming surfaces for this case. The
engine drives the streams; advanced consumers can still hit the raw
API directly.
