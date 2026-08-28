# bifrost-graph: hunt findings

Scope: `crates/graph/` - Microsoft Graph delta-token sync, webhook push with
renewal health worker plus EWS streaming fallback, cursor envelope and validation,
`If-Match` etag mutations, error mapping, public-folder path.

**Sizes worth flagging as a smell** (low confidence as defects, high as maintenance
risk): `pim.rs` is 4,795 lines and `push.rs` 2,600. `pim.rs` in particular is
holding message writes, drafts, send-as, search + its cursor codec, folder CRUD,
identities, vacation, and typed hydration in one file; the search cursor logic alone
(1,984-2,150) is a self-contained subsystem with its own versioned wire format.

## Out of scope, flagged

- `bifrost-google`'s `calendars_list` is cited in `paging.rs` as having learned the
  same lesson independently. Worth checking whether Google's *delta/history* walks
  got the guard, or only its list walks - the miss here was exactly that split.
