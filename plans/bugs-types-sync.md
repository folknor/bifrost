# Bug hunt: bifrost-types + bifrost-sync (2026-07-28)

Scope: `crates/types/src/**`, `crates/sync/src/**`,
`crates/sync/tests/**`, `reference/error-model.md`, and
`reference/sync.md`.

## Current gaps

Only unresolved findings belong in this ledger. Durable behavior and
the tests that pin former findings are recorded in `reference/sync.md`.

- **N3. Future `EngineDirective` variants default account-wide in
  `directive_target_scope`.** The enum is `#[non_exhaustive]` in the
  `bifrost-types` dependency, so the wildcard is required here and cannot
  be replaced with a compile-time-exhaustive match. A future scope-bearing
  directive needs an explicit arm in both `directive_target_scope` and
  `DirectiveKey`; until then it is routed and deduped account-wide.
