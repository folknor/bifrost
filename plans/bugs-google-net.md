# bugs-google-net - bifrost-google + bifrost-net sweep

No confirmed open findings remain from the sweep over
`crates/google/src/**`, `crates/net/src/**`, and `crates/net/tests/**`.

This document contains only current gaps. Resolved findings and their
landed tests are represented by the code and reference documents, not
retained here as historical discrepancies.

## Audit boundaries

The following areas were not part of the original detailed coverage
pass and have not received a new line-by-line audit in this plan:

- Gmail MIME rendering, draft patching, search translation, identity
  and vacation mapping in `crates/google/src/account/pim.rs`.
- The already test-dense contacts, calendar, account-error, filters,
  and cloud modules.
- `crates/net/src/account_error.rs` beyond the existing integration
  suite, and `trace.rs` beyond its construction-level invariants.
- `bifrost-graph` beyond its `attach_account` reattach path. That path
  was audited and fixed here because the `bifrost-net` registration
  token change removed the self-healing re-attach that graph had been
  relying on; the rest of the crate's account lifecycle was not
  re-read.

These are audit boundaries, not known defects.
