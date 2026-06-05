use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, ConvenienceShape, CursorFreshness,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape,
};

use crate::types::{Capability, MailboxAttribute, MailboxInfo, ServerProfile};

pub(crate) fn build_capabilities(
    profile: &ServerProfile,
    folders: &[MailboxInfo],
    has_sieve: bool,
    has_carddav: bool,
    has_caldav: bool,
) -> AccountCapabilities {
    let has_drafts = folders.iter().any(|folder| {
        folder
            .attributes
            .iter()
            .any(|attr| matches!(attr, MailboxAttribute::Drafts))
            || folder.name.as_str().eq_ignore_ascii_case("drafts")
    });
    let has_thread_references = profile.capabilities.iter().any(
        |cap| matches!(cap, Capability::Thread(alg) if alg.eq_ignore_ascii_case("REFERENCES")),
    );
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        blob_range: BlobRangeSupport::Yes,
        blob_digest_pre_download: false,
        push: if profile.supports(Capability::Idle) {
            PushCapability::InProcess
        } else {
            PushCapability::None
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 1024,
            max_wait: Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::PerConnection,
        quota_signal: if profile.supports(Capability::Quota) {
            QuotaSignal::Implicit
        } else {
            QuotaSignal::None
        },
        requires_uidvalidity_recheck: true,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            add_to_container: true,
            remove_from_container: true,
            set_keyword: true,
            set_label_membership: false,
            set_category: false,
            set_extended_property: false,
            set_is_read: true,
            send_message: false,
            attachment_upload: false,
            draft_create: has_drafts && profile.supports(Capability::UidPlus),
            draft_update: false,
            draft_discard: true,
            draft_send: false,
            search: has_thread_references,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: false,
            identity_update: false,
            vacation_get: false,
            vacation_set: false,
            quota_get: profile.supports(Capability::Quota),
            thread_hydrate: has_thread_references,
            message_hydrate: true,
            filters_list: has_sieve,
            filter_create: has_sieve,
            filter_update: has_sieve,
            filter_delete: has_sieve,
            filter_validate: has_sieve,
            address_books_list: has_carddav,
            contacts_list: has_carddav,
            contact_get: has_carddav,
            contact_create: has_carddav,
            contact_update: has_carddav,
            contact_delete: has_carddav,
            contact_search: has_carddav,
            contact_autocomplete: has_carddav,
            calendars_list: has_caldav,
            events_in_range: has_caldav,
            event_get: has_caldav,
            event_create: has_caldav,
            event_update: has_caldav,
            event_delete: has_caldav,
            event_rsvp: has_caldav,
            event_search: has_caldav,
            event_autocomplete: has_caldav,
        },
        filter_rule_shape: if has_sieve {
            FilterRuleShape::Scripts
        } else {
            FilterRuleShape::None
        },
        conveniences: ConvenienceShape {
            starred: StarredFlagShape::Keyword,
            replied_via_keyword: true,
            replied_via_extended_property: false,
            forwarded_via_keyword: true,
            forwarded_via_extended_property: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_builder_sets_imap_tier_values() {
        let profile = ServerProfile::new(
            vec![Capability::Idle, Capability::Condstore, Capability::Quota],
            Vec::new(),
        );
        let caps = build_capabilities(&profile, &[], false, false, false);
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.blob_range, BlobRangeSupport::Yes);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::None);
        assert!(!caps.pim_methods.filters_list);
        assert!(!caps.pim_methods.address_books_list);
        assert!(!caps.pim_methods.contacts_list);
        assert!(!caps.pim_methods.contact_get);
        assert!(!caps.pim_methods.contact_create);
        assert!(!caps.pim_methods.contact_update);
        assert!(!caps.pim_methods.contact_delete);
        assert!(!caps.pim_methods.contact_search);
        assert!(!caps.pim_methods.contact_autocomplete);
        assert!(caps.requires_uidvalidity_recheck);
    }

    #[test]
    fn capability_builder_leaves_mutation_concurrency_none_without_condstore() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], false, false, false);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
    }

    #[test]
    fn capability_builder_advertises_sieve_when_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], true, false, false);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Scripts);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_create);
        assert!(caps.pim_methods.filter_update);
        assert!(caps.pim_methods.filter_delete);
        assert!(caps.pim_methods.filter_validate);
    }

    #[test]
    fn capability_builder_advertises_contacts_when_carddav_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], false, true, false);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
    }
}
