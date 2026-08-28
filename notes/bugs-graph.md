# bifrost-graph: hunt findings

Scope: `crates/graph/` - Microsoft Graph delta-token sync, webhook push with
renewal health worker plus EWS streaming fallback, cursor envelope and validation,
`If-Match` etag mutations, error mapping, public-folder path.

**Sizes worth flagging as a smell** (low confidence as defects, high as maintenance
risk): `pim.rs` is 4,795 lines and `push.rs` 2,600. `pim.rs` in particular is
holding message writes, drafts, send-as, search + its cursor codec, folder CRUD,
identities, vacation, and typed hydration in one file; the search cursor logic alone
(1,984-2,150) is a self-contained subsystem with its own versioned wire format.

## Round 1 residuals

- **Finding 1 (`PageWalk` on the delta inventory and changes walks) landed with
  no test of its own.** The guard is wired at both walks and the enumeration in
  `reference/graph.md` is accurate, but nothing pins that a delta server
  repeating a `nextLink` is refused - only the folder-list walk has such a test
  (`a_repeated_next_link_refuses_the_folder_walk`). Not a defect; a hole a later
  refactor can reopen silently.
- Three tests this round shipped WITHOUT biting the defect they were written for
  and were strengthened in place: the webhook renewal test asserted only that the
  stored expiry was not the stale one (true of the locally computed expiry it was
  meant to rule out), and the search-resume test round-tripped the new cursor
  codec without ever exercising a page that over-delivers. Both now pin the real
  behaviour, the search one through `contacts::search` against a scripted page.

## Out of scope, flagged

- `bifrost-google`'s `calendars_list` is cited in `paging.rs` as having learned the
  same lesson independently. Worth checking whether Google's *delta/history* walks
  got the guard, or only its list walks - the miss here was exactly that split.
