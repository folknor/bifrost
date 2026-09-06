//! Per-item routing of a `$batch` chunk.
//!
//! Every Graph `$batch` chunk is built by turning each submitted `ObjectId`
//! into one subrequest URL, and that construction can fail locally: a
//! foreign-encoded id whose shared mailbox is no longer configured has no
//! endpoint to address, so `client_for_owner` refuses it rather than
//! stripping the owner and sending the id to `/me` (a different mailbox
//! namespace).
//!
//! The lane rule that follows is the crate's oldest recurring mistake, twice
//! relearned: a surface that answers per item must not answer per request. A
//! `?` in the chunk builder discards every valid sibling of the bad id - and
//! on a `BatchOutcome` surface it also contradicts the boundary contract that
//! a top-level `Err` means nothing was transmitted, since earlier chunks may
//! already have completed. So the builder PARTITIONS: routable ids go to the
//! wire, unroutable ids go to the failed lane, and the chunk proceeds.
//!
//! The partition is pure and generic over the per-site URL builder, so the
//! rule itself is pinnable here; the round trip the routable half then
//! makes is pinned at each call site through the `GraphClient` REST seam.
//!
//! What is deliberately NOT shared: the per-item lane discipline downstream
//! of this partition - the `reconcile_*` readers and `BatchOutcomeBuilder`
//! use - exists four times (hydration, the mutation funnel, reactions, push),
//! and `resolve_batch_responses` / `reconcile_hydration_responses` each carry
//! their own invalid-index and duplicate-index rules. A validated `$batch`
//! projection type replacing those copies was proposed in the 2026-09-04
//! bug hunt and ruled NOT approved by the repository owner: one recorded
//! drift between the four is thin evidence for restructuring four working
//! lanes. Re-raise it if a second drift between them is ever found.

use bifrost_types::ObjectId;

/// The two halves of a routed chunk: the `(id, built subrequest)` pairs
/// that go on the wire, and the `(id, routing failure)` pairs that go
/// straight to the failed lane. Both keep submission order.
pub(crate) type RoutedChunk<T, E> = (Vec<(ObjectId, T)>, Vec<(ObjectId, E)>);

/// Split a `$batch` chunk into the ids whose subrequest was built and the
/// ids whose subrequest could not be built at all, preserving submission
/// order in both halves.
///
/// `build` is the call site's URL/request constructor; its error type is
/// whatever that site classifies with. Ownership of the failure stays with
/// the caller precisely because the three call sites file it differently
/// (`ItemOutcome::Failed` into a `Batch`, `push_failed` into a
/// `BatchOutcomeBuilder`) - what is shared, and what this function fixes in
/// place, is that neither may propagate it out of the chunk.
pub(crate) fn partition_routable<T, E>(
    ids: &[ObjectId],
    mut build: impl FnMut(&ObjectId) -> Result<T, E>,
) -> RoutedChunk<T, E> {
    let mut routable = Vec::with_capacity(ids.len());
    let mut rejected = Vec::new();
    for id in ids {
        match build(id) {
            Ok(built) => routable.push((id.clone(), built)),
            Err(error) => rejected.push((id.clone(), error)),
        }
    }
    (routable, rejected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(value: &str) -> ObjectId {
        ObjectId(value.to_string())
    }

    /// The rule the three `$batch` builders exist to obey: one unroutable id
    /// costs itself an outcome and nothing else. Before this, the builders
    /// used `?`, so a single stale shared-mailbox id took the whole chunk
    /// (and, on the reaction surface, every chunk already transmitted before
    /// it) down with it.
    #[test]
    fn an_unroutable_id_does_not_take_its_siblings_with_it() {
        let ids = [oid("a"), oid("stale"), oid("b")];
        let (routable, rejected) = partition_routable(&ids, |id| {
            if id.0 == "stale" {
                Err("no such shared mailbox")
            } else {
                Ok(format!("/me/messages/{}", id.0))
            }
        });
        let routed: Vec<&str> = routable.iter().map(|(_, url)| url.as_str()).collect();
        assert_eq!(routed, ["/me/messages/a", "/me/messages/b"]);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, oid("stale"));
    }

    /// Submission order survives the split on both sides. The routable half
    /// is what the subrequest index (`BatchRequestItem::id`) is assigned
    /// from, so a reordering here would silently misattribute every response
    /// past the first rejection.
    #[test]
    fn submission_order_is_preserved_in_both_halves() {
        let ids = [oid("1"), oid("x"), oid("2"), oid("y"), oid("3")];
        let (routable, rejected) = partition_routable(&ids, |id| {
            if id.0.parse::<u8>().is_ok() {
                Ok(())
            } else {
                Err(())
            }
        });
        let routed: Vec<&str> = routable.iter().map(|(id, ())| id.0.as_str()).collect();
        let refused: Vec<&str> = rejected.iter().map(|(id, ())| id.0.as_str()).collect();
        assert_eq!(routed, ["1", "2", "3"]);
        assert_eq!(refused, ["x", "y"]);
    }

    /// Every submitted id lands in exactly one half - the accounting
    /// invariant the caller's `finalize` / one-outcome-per-id sweep then
    /// depends on.
    #[test]
    fn every_submitted_id_lands_in_exactly_one_half() {
        let ids = [oid("a"), oid("b"), oid("c")];
        let (routable, rejected) =
            partition_routable(&ids, |id| if id.0 == "b" { Err(()) } else { Ok(()) });
        assert_eq!(routable.len() + rejected.len(), ids.len());
    }

    /// A chunk in which nothing routes yields an EMPTY routable half, which
    /// is the signal the call sites use to skip the `$batch` POST entirely.
    /// Posting an empty `requests` array is a 400 from Graph, and the
    /// rejected ids already have their outcomes.
    #[test]
    fn a_wholly_unroutable_chunk_leaves_nothing_to_send() {
        let ids = [oid("a"), oid("b")];
        let (routable, rejected) = partition_routable(&ids, |_| Err::<(), _>(()));
        assert!(routable.is_empty());
        assert_eq!(rejected.len(), 2);
    }
}
