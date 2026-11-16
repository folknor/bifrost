//! Exact evaluation of a search filter that contains `SearchFilter::Has`.
//!
//! IMAP SEARCH cannot see MIME part headers or parameters: no portable
//! search key matches an attachment's filename or media type, and `BODY`
//! matches prose. So a filter with `Has` leaves is evaluated per folder as
//! set algebra over UIDs:
//!
//! 1. The universe `U` is one SEARCH for an over-approximation of the whole
//!    filter (every `Has` and every negated attachment subtree widened to
//!    `ALL`) conjoined with the provider query.
//! 2. Every attachment-free subtree is one SEARCH of its own, intersected
//!    with `U`. Identical subtrees share one command.
//! 3. `Has` leaves are evaluated from a `UID FETCH (UID BODYSTRUCTURE)` over
//!    `U`, in bounded chunks, aggregated per requested UID across the whole
//!    command (an unsolicited FLAGS update for the same UID can arrive before
//!    the BODYSTRUCTURE).
//! 4. The boolean tree is evaluated over `U`, with `Not` as the complement
//!    relative to `U`.
//!
//! A requested UID the FETCH returned nothing for no longer exists (UID
//! FETCH silently omits expunged UIDs), so it leaves `U` and therefore every
//! result, including the complement of a `Has`. A UID the server did answer
//! for but never described is incomplete provider data and fails the
//! folder: a missing observation must never read as "has no attachment".
//!
//! The result is exact for the leaf memberships observed. The commands are
//! not one atomic snapshot of the mailbox; the paging contract does not ask
//! for one.

use std::collections::{BTreeSet, HashMap};

use super::FilterNode;
use crate::connection::ImapConnection;
use crate::types::{FetchAttr, SearchCriteria, ThreadNode, UidSetExpr};

/// UIDs per BODYSTRUCTURE FETCH. A fragmented universe compresses poorly into
/// a sequence set, and servers bound command lines; 256 singletons stay well
/// under the 8000-octet floor RFC 2683 recommends servers accept.
const FETCH_CHUNK: usize = 256;

/// Which result surface is being evaluated. It decides how a criteria SEARCH
/// is issued, so each surface keeps the charset handling it has without
/// attachments: message search uses `UID SEARCH`, thread search uses
/// `UID THREAD REFERENCES UTF-8` and flattens the members.
#[derive(Debug, Clone, Copy)]
pub(super) enum Surface {
    Messages,
    Threads,
}

/// The exact set of matching UIDs in the selected folder.
pub(super) async fn matching_uids(
    conn: &ImapConnection,
    surface: Surface,
    tree: &FilterNode,
    universe: &SearchCriteria,
    timeout: std::time::Duration,
) -> Result<BTreeSet<u32>, crate::Error> {
    let mut universe_uids = criteria_uids(conn, surface, universe, timeout).await?;

    let mut values = Vec::new();
    let mut criteria = Vec::new();
    collect_leaves(tree, &mut values, &mut criteria);

    let mut has_sets: HashMap<String, BTreeSet<u32>> = HashMap::new();
    if !values.is_empty() && !universe_uids.is_empty() {
        let structures = fetch_attachments(conn, &universe_uids, timeout).await?;
        universe_uids.retain(|uid| structures.contains_key(uid));
        for value in values {
            let set = structures
                .iter()
                .filter(|(_, entities)| crate::account::parts::attachments_match(entities, &value))
                .map(|(uid, _)| *uid)
                .collect();
            has_sets.insert(value, set);
        }
    }

    let mut server_sets: HashMap<String, BTreeSet<u32>> = HashMap::new();
    for leaf in criteria {
        if server_sets.contains_key(leaf.as_str()) {
            continue;
        }
        let uids = criteria_uids(conn, surface, &leaf, timeout).await?;
        server_sets.insert(leaf.as_str().to_owned(), uids);
    }

    Ok(evaluate(tree, &universe_uids, &has_sets, &server_sets))
}

/// Set-algebra evaluation of the tree over `universe`. Every leaf set is
/// intersected with the universe, so a UID outside it can never surface.
pub(super) fn evaluate(
    node: &FilterNode,
    universe: &BTreeSet<u32>,
    has_sets: &HashMap<String, BTreeSet<u32>>,
    server_sets: &HashMap<String, BTreeSet<u32>>,
) -> BTreeSet<u32> {
    let within = |set: Option<&BTreeSet<u32>>| {
        set.map_or_else(BTreeSet::new, |set| {
            set.intersection(universe).copied().collect()
        })
    };
    match node {
        FilterNode::Server(criteria) => within(server_sets.get(criteria.as_str())),
        FilterNode::Has(value) => within(has_sets.get(value)),
        FilterNode::And(nodes) => nodes.iter().fold(universe.clone(), |acc, node| {
            let next = evaluate(node, universe, has_sets, server_sets);
            acc.intersection(&next).copied().collect()
        }),
        // The compiler never builds an empty Or here (it compiles to ALL);
        // an empty union is still the honest answer for the shape.
        FilterNode::Or(nodes) => nodes.iter().fold(BTreeSet::new(), |mut acc, node| {
            acc.extend(evaluate(node, universe, has_sets, server_sets));
            acc
        }),
        FilterNode::Not(inner) => {
            let inner = evaluate(inner, universe, has_sets, server_sets);
            universe.difference(&inner).copied().collect()
        }
    }
}

fn collect_leaves(node: &FilterNode, values: &mut Vec<String>, criteria: &mut Vec<SearchCriteria>) {
    match node {
        FilterNode::Server(leaf) => criteria.push(leaf.clone()),
        FilterNode::Has(value) => {
            if !values.contains(value) {
                values.push(value.clone());
            }
        }
        FilterNode::And(nodes) | FilterNode::Or(nodes) => {
            for node in nodes {
                collect_leaves(node, values, criteria);
            }
        }
        FilterNode::Not(inner) => collect_leaves(inner, values, criteria),
    }
}

async fn criteria_uids(
    conn: &ImapConnection,
    surface: Surface,
    criteria: &SearchCriteria,
    timeout: std::time::Duration,
) -> Result<BTreeSet<u32>, crate::Error> {
    match surface {
        Surface::Messages => Ok(conn
            .uid_search(criteria.clone(), timeout)
            .await?
            .ids
            .into_iter()
            .filter(|uid| *uid != 0)
            .collect()),
        Surface::Threads => {
            let roots = conn
                .uid_thread("REFERENCES", "UTF-8", criteria.clone(), timeout)
                .await?;
            Ok(thread_members(&roots))
        }
    }
}

fn thread_members(roots: &[ThreadNode]) -> BTreeSet<u32> {
    let mut uids = Vec::new();
    for root in roots {
        super::flatten_thread(root, &mut uids);
    }
    uids.into_iter().filter(|uid| *uid != 0).collect()
}

/// The attachment entities of every UID in `uids` that still exists.
async fn fetch_attachments(
    conn: &ImapConnection,
    uids: &BTreeSet<u32>,
    timeout: std::time::Duration,
) -> Result<HashMap<u32, Vec<crate::account::parts::AttachmentEntity>>, crate::Error> {
    let requested: Vec<u32> = uids.iter().copied().collect();
    let mut described = HashMap::new();
    for chunk in requested.chunks(FETCH_CHUNK) {
        let mut answered = BTreeSet::new();
        let set =
            UidSetExpr::from_uids(chunk.iter().filter_map(|uid| crate::types::Uid::new(*uid)))
                .expect("chunks are non-empty and hold nonzero UIDs");
        let (mut rx, fetch_fut) = conn.uid_fetch_stream(
            set.as_sequence_set(),
            &[FetchAttr::Uid, FetchAttr::BodyStructure],
            timeout,
        )?;
        tokio::pin!(fetch_fut);
        let mut fetch_result = None;
        loop {
            tokio::select! {
                result = &mut fetch_fut, if fetch_result.is_none() => {
                    fetch_result = Some(result);
                }
                item = rx.recv() => match item {
                    Some(fetch) => {
                        let fetch = fetch?;
                        // Unsolicited or out-of-set responses are not
                        // observations of what was asked for.
                        let Some(uid) = fetch.uid.filter(|uid| chunk.binary_search(uid).is_ok()) else {
                            continue;
                        };
                        answered.insert(uid);
                        if let Some(structure) = fetch.body_structure.as_ref() {
                            described.insert(uid, crate::account::parts::attachment_entities(structure));
                        }
                    }
                    None => {
                        // The driver closes the stream before it answers the
                        // completion, so a failed FETCH can close `rx` first:
                        // await the completion before trusting the stream.
                        match fetch_result.take() {
                            Some(result) => result?,
                            None => (&mut fetch_fut).await?,
                        }
                        break;
                    }
                }
            }
        }
        if let Some(uid) = answered.iter().find(|uid| !described.contains_key(*uid)) {
            return Err(crate::Error::Provider(
                crate::error::ProviderFailure::Missing(format!(
                    "FETCH answered UID {uid} without the requested BODYSTRUCTURE"
                )),
            ));
        }
    }
    Ok(described)
}

#[cfg(test)]
#[path = "attachment_tests.rs"]
mod tests;
