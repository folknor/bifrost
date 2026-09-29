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

use crate::client::GraphClient;
use crate::error::GraphError;
use crate::origin::AdmittedUrl;

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

    /// Admit one page URL, or refuse the walk. Keyed on the admitted URL's
    /// serialization, so two spellings of one link count as a repeat.
    pub(crate) fn enter(&mut self, url: &AdmittedUrl) -> Result<(), GraphError> {
        self.pages += 1;
        if self.pages > MAX_PAGES {
            return Err(GraphError::Json {
                message: format!("Graph {} pagination exceeded {MAX_PAGES} pages", self.what),
                body: None,
            });
        }
        if !self.seen.insert(url.as_str().to_string()) {
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

/// Decode a caller's paged cursor (minted by [`encode_paged_cursor`]) into
/// an admitted URL and the count of already-delivered values to skip.
///
/// These bytes are request INPUT the caller handed back, so every refusal
/// is a detail for `Request(Malformed)`, never a provider fault. The url
/// must be absolute, or a path starting with `/` (this crate once minted
/// first-page positions that way), and either way must admit onto the
/// client's api-base: a cursor naming another origin would otherwise be
/// fetched with the account bearer. A cursor that is not the JSON form is
/// read as a bare link cursor, which must be an absolute URL.
pub(crate) fn decode_paged_cursor(
    client: &GraphClient,
    cursor: Option<Vec<u8>>,
) -> Result<Option<(AdmittedUrl, usize)>, String> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    match serde_json::from_slice::<PagedCursor>(&cursor) {
        Ok(PagedCursor { url, skip }) => {
            if !url.starts_with('/') && reqwest::Url::parse(&url).is_err() {
                return Err("page cursor url is neither absolute nor a path".to_string());
            }
            let url = client
                .admit_target(&url)
                .map_err(|refusal| format!("page cursor {refusal}"))?;
            Ok(Some((url, skip)))
        }
        Err(_) => Ok(decode_link_cursor(client, Some(cursor))?.map(|url| (url, 0))),
    }
}

/// Decode a caller's bare link cursor: the verbatim `@odata.nextLink` bytes
/// a list surface returned as `next_cursor`. Strict UTF-8, an absolute URL,
/// and admitted onto the client's api-base; refusals are details for
/// `Request(Malformed)`.
pub(crate) fn decode_link_cursor(
    client: &GraphClient,
    cursor: Option<Vec<u8>>,
) -> Result<Option<AdmittedUrl>, String> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let text = String::from_utf8(cursor).map_err(|error| format!("page cursor: {error}"))?;
    if reqwest::Url::parse(&text).is_err() {
        return Err("page cursor is not an absolute URL".to_string());
    }
    client
        .admit_target(&text)
        .map(Some)
        .map_err(|refusal| format!("page cursor {refusal}"))
}

pub(crate) fn encode_paged_cursor(url: &AdmittedUrl, skip: usize) -> Vec<u8> {
    serde_json::to_vec(&PagedCursor {
        url: url.as_str().to_string(),
        skip,
    })
    .expect("search cursor is serializable")
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_PAGES, PageWalk, decode_link_cursor, decode_paged_cursor, encode_paged_cursor,
    };
    use crate::client::GraphClient;
    use crate::origin::{AdmittedUrl, Base};

    fn url(link: &str) -> AdmittedUrl {
        Base::parse("https://graph.test")
            .admit(link)
            .expect("same origin")
    }

    #[test]
    fn search_cursor_resumes_inside_an_overdelivered_page() {
        let client = GraphClient::new("token");
        let link = client
            .admit_target("https://graph.microsoft.com/v1.0/users?page=4")
            .expect("same origin");
        let encoded = encode_paged_cursor(&link, 17);
        let (url, skip) = decode_paged_cursor(&client, Some(encoded))
            .expect("valid cursor")
            .expect("present cursor");
        assert_eq!(url, link);
        assert_eq!(skip, 17);
    }

    /// A cursor minted before cursors carried absolute urls names its first
    /// page as a path; it still resumes, on the client's own base.
    #[test]
    fn a_path_paged_cursor_resumes_on_the_api_base() {
        let client = GraphClient::new("token");
        let cursor = br#"{"url":"/me/events?$top=5","skip":2}"#.to_vec();
        let (url, skip) = decode_paged_cursor(&client, Some(cursor))
            .expect("valid cursor")
            .expect("present cursor");
        assert_eq!(
            url.as_str(),
            "https://graph.microsoft.com/v1.0/me/events?$top=5"
        );
        assert_eq!(skip, 2);
    }

    /// The token leak: a caller's cursor bytes naming another origin were
    /// turned into a request URL verbatim and fetched with the bearer.
    #[test]
    fn a_cursor_off_the_api_origin_is_refused() {
        let client = GraphClient::new("token");
        for cursor in [
            br#"{"url":"https://elsewhere.example/users","skip":0}"#.to_vec(),
            br#"{"url":"HTTPS://elsewhere.example/users","skip":0}"#.to_vec(),
            br#"{"url":"https://graph.microsoft.com@elsewhere.example/v1.0","skip":0}"#.to_vec(),
            b"https://elsewhere.example/users".to_vec(),
            b"HTTPS://elsewhere.example/users".to_vec(),
        ] {
            assert!(decode_paged_cursor(&client, Some(cursor.clone())).is_err());
            assert!(decode_link_cursor(&client, Some(cursor)).is_err());
        }
    }

    /// Bytes this crate could not have minted are refused, not turned into a
    /// same-origin request for whatever path they spell.
    #[test]
    fn unmintable_cursor_bytes_are_refused() {
        let client = GraphClient::new("token");
        for cursor in [
            b"garbage".to_vec(),
            b"me/messages".to_vec(),
            vec![0xff, 0xfe],
            br#"{"url":"garbage","skip":0}"#.to_vec(),
        ] {
            assert!(decode_paged_cursor(&client, Some(cursor.clone())).is_err());
            assert!(decode_link_cursor(&client, Some(cursor)).is_err());
        }
    }

    #[test]
    fn a_repeated_link_is_refused() {
        let mut walk = PageWalk::new("calendars");
        walk.enter(&url("https://graph.test/calendars"))
            .expect("first");
        walk.enter(&url("https://graph.test/calendars?page=2"))
            .expect("second");
        let error = walk
            .enter(&url("https://graph.test/calendars"))
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
            walk.enter(&url(&format!("https://graph.test/c?page={page}")))
                .expect("within budget");
        }
        let error = walk
            .enter(&url("https://graph.test/c?page=never-seen-before"))
            .expect_err("an unrepeated but endless walk must still be bounded");
        assert!(format!("{error:?}").contains("exceeded"));
    }
}
