//! Bounded `@odata.nextLink` traversal.
//!
//! Every Graph collection walk follows server-supplied `nextLink` values until
//! the server stops sending them. That is an unbounded loop against a remote:
//! a server that keeps emitting a link - through a bug, a proxy, or a hostile
//! response - spins the walk forever while the accumulating `Vec` grows without
//! limit. Six such loops existed here with no bound of any kind.
//!
//! `PageWalk` is the shared guard. It is deliberately ONE type rather than a
//! copy of the check at each site: the same class of defect was fixed
//! independently in `bifrost-google`'s `calendars_list`, and the lesson there is
//! that both halves are needed. A repeated-link check alone does not stop a
//! server handing out a FRESH link every page, and a budget alone lets a tight
//! two-link cycle burn the whole budget on requests. Only the pair bounds both
//! shapes.
//!
//! A refusal is a provider-contract violation, not a transport failure, so it
//! travels as `GraphError::Json` - the crate's established carrier for "the
//! response did not match the documented contract", classifying as
//! `Protocol(ParseFailed)` / `Wire(MalformedResponse)`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::error::GraphError;

/// Maximum pages one collection walk may request.
///
/// Deliberately generous: at Graph's typical `$top` of 100-250 this is
/// 1,000,000+ objects, so no real mailbox, calendar set, or contact folder
/// tree approaches it. The budget exists to bound a MISBEHAVING server, not to
/// limit legitimate data, and a walk that hits it has learned something is
/// wrong rather than that the account is large.
const MAX_PAGES: usize = 10_000;

/// Guard for a single `nextLink` traversal.
///
/// Call [`PageWalk::enter`] with each URL before fetching it, including the
/// first. Checking the first URL too means a server that echoes the request URI
/// back as its own `nextLink` is caught on the second pass rather than looping.
pub(crate) struct PageWalk {
    seen: HashSet<String>,
    pages: usize,
    /// Names the collection in a refusal, so a support report says WHICH walk
    /// the server misbehaved on.
    what: &'static str,
}

impl PageWalk {
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            seen: HashSet::new(),
            pages: 0,
            what,
        }
    }

    /// Admit one page URL, or refuse the walk.
    pub(crate) fn enter(&mut self, url: &str) -> Result<(), GraphError> {
        self.pages += 1;
        if self.pages > MAX_PAGES {
            return Err(GraphError::Json {
                message: format!("Graph {} pagination exceeded {MAX_PAGES} pages", self.what),
                body: None,
            });
        }
        if !self.seen.insert(url.to_string()) {
            return Err(GraphError::Json {
                message: format!("Graph {} pagination repeated a page link", self.what),
                body: None,
            });
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PagedCursor {
    url: String,
    skip: usize,
}

pub(crate) fn decode_paged_cursor(cursor: Option<Vec<u8>>) -> (Option<String>, usize) {
    let Some(cursor) = cursor else {
        return (None, 0);
    };
    match serde_json::from_slice::<PagedCursor>(&cursor) {
        Ok(cursor) => (Some(cursor.url), cursor.skip),
        Err(_) => (Some(String::from_utf8_lossy(&cursor).into_owned()), 0),
    }
}

pub(crate) fn encode_paged_cursor(url: String, skip: usize) -> Vec<u8> {
    serde_json::to_vec(&PagedCursor { url, skip }).expect("search cursor is serializable")
}

#[cfg(test)]
mod tests {
    use super::{MAX_PAGES, PageWalk, decode_paged_cursor, encode_paged_cursor};

    #[test]
    fn search_cursor_resumes_inside_an_overdelivered_page() {
        let encoded = encode_paged_cursor("https://graph.test/users?page=4".to_string(), 17);
        let (url, skip) = decode_paged_cursor(Some(encoded));
        assert_eq!(url.as_deref(), Some("https://graph.test/users?page=4"));
        assert_eq!(skip, 17);
    }

    #[test]
    fn a_repeated_link_is_refused() {
        let mut walk = PageWalk::new("calendars");
        walk.enter("https://graph.test/calendars").expect("first");
        walk.enter("https://graph.test/calendars?page=2")
            .expect("second");
        let error = walk
            .enter("https://graph.test/calendars")
            .expect_err("a link already walked must be refused");
        assert!(format!("{error:?}").contains("repeated a page link"));
    }

    /// The budget is the half a repeated-link check cannot cover: a server
    /// handing out a FRESH link every page never repeats itself, so only a
    /// finite budget stops it. This is exactly how the same defect was closed
    /// in bifrost-google, and why both guards are here rather than one.
    #[test]
    fn a_server_issuing_endless_fresh_links_is_refused() {
        let mut walk = PageWalk::new("contactFolders");
        for page in 0..MAX_PAGES {
            walk.enter(&format!("https://graph.test/c?page={page}"))
                .expect("within budget");
        }
        let error = walk
            .enter("https://graph.test/c?page=never-seen-before")
            .expect_err("an unrepeated but endless walk must still be bounded");
        assert!(format!("{error:?}").contains("exceeded"));
    }
}
