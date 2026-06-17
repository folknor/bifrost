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
    contacts: Option<&AccountCapabilities>,
    calendars: Option<&AccountCapabilities>,
    submission_configured: bool,
) -> AccountCapabilities {
    // Copy only the contact / calendar field subsets from the
    // sub-accounts' real capability snapshots; an absent sub-account
    // leaves those flags `false` (`PimMethodSupport::default()`). Every
    // other `pim_methods` field stays IMAP-owned below.
    let contact_pim = contacts.map(|c| &c.pim_methods);
    let calendar_pim = calendars.map(|c| &c.pim_methods);
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
            set_importance: true,
            set_is_read: true,
            send_message: submission_configured,
            attachment_upload: false,
            host_attachment: false,
            draft_create: has_drafts && profile.supports(Capability::UidPlus),
            draft_update: false,
            draft_discard: true,
            draft_send: submission_configured,
            // IMAP relay FUTURERELEASE is per-connection (advertised in
            // EHLO at send time), unknown at open. The honest snapshot
            // is `false`; an unsupporting relay surfaces a runtime
            // `Unsupported(Send)` from the smtp boundary.
            scheduled_send: false,
            // Graph-style shared-mailbox send routing is not modeled over
            // SMTP; a shared-mailbox send is `request.from` + relay
            // authorization. A `send_as` request is rejected at the
            // boundary.
            send_as: false,
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
            open_raw_rfc822: true,
            filters_list: has_sieve,
            filter_create: has_sieve,
            filter_update: has_sieve,
            filter_delete: has_sieve,
            filter_validate: has_sieve,
            address_books_list: contact_pim.is_some_and(|p| p.address_books_list),
            contacts_list: contact_pim.is_some_and(|p| p.contacts_list),
            contact_get: contact_pim.is_some_and(|p| p.contact_get),
            contact_create: contact_pim.is_some_and(|p| p.contact_create),
            contact_update: contact_pim.is_some_and(|p| p.contact_update),
            contact_delete: contact_pim.is_some_and(|p| p.contact_delete),
            contact_search: contact_pim.is_some_and(|p| p.contact_search),
            contact_autocomplete: contact_pim.is_some_and(|p| p.contact_autocomplete),
            directory_search: false,
            calendars_list: calendar_pim.is_some_and(|p| p.calendars_list),
            events_in_range: calendar_pim.is_some_and(|p| p.events_in_range),
            event_get: calendar_pim.is_some_and(|p| p.event_get),
            event_create: calendar_pim.is_some_and(|p| p.event_create),
            event_update: calendar_pim.is_some_and(|p| p.event_update),
            event_delete: calendar_pim.is_some_and(|p| p.event_delete),
            event_rsvp: calendar_pim.is_some_and(|p| p.event_rsvp),
            event_search: calendar_pim.is_some_and(|p| p.event_search),
            event_autocomplete: calendar_pim.is_some_and(|p| p.event_autocomplete),
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
            mdn_sent_via_keyword: true,
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
        let caps = build_capabilities(&profile, &[], false, None, None, false);
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
        assert!(caps.pim_methods.open_raw_rfc822);
        assert!(caps.pim_methods.set_importance);
        assert!(caps.conveniences.mdn_sent_via_keyword);
        assert!(caps.requires_uidvalidity_recheck);
    }

    #[test]
    fn capability_builder_leaves_mutation_concurrency_none_without_condstore() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], false, None, None, false);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
    }

    #[test]
    fn capability_builder_advertises_sieve_when_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], true, None, None, false);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Scripts);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_create);
        assert!(caps.pim_methods.filter_update);
        assert!(caps.pim_methods.filter_delete);
        assert!(caps.pim_methods.filter_validate);
    }

    /// Minimal capability snapshot for a composed sub-account, with all
    /// contact flags on and all calendar flags off (the carddav shape).
    fn contacts_snapshot() -> AccountCapabilities {
        let mut caps = crate::account::test_support::stub_capabilities();
        caps.pim_methods.address_books_list = true;
        caps.pim_methods.contacts_list = true;
        caps.pim_methods.contact_get = true;
        caps.pim_methods.contact_create = true;
        caps.pim_methods.contact_update = true;
        caps.pim_methods.contact_delete = true;
        caps.pim_methods.contact_search = true;
        caps.pim_methods.contact_autocomplete = true;
        caps
    }

    #[test]
    fn capability_builder_advertises_contacts_when_carddav_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let sub = contacts_snapshot();
        let caps = build_capabilities(&profile, &[], false, Some(&sub), None, false);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
        // Calendar flags stay false with no calendars sub.
        assert!(!caps.pim_methods.calendars_list);
        assert!(!caps.pim_methods.event_rsvp);
    }

    #[test]
    fn capability_builder_copies_real_sub_account_flags() {
        // A calendars sub that does NOT support event_rsvp must yield a
        // composed capability with event_rsvp = false, proving the
        // snapshot is copied field-by-field rather than blanket-true.
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let mut calendars = crate::account::test_support::stub_capabilities();
        calendars.pim_methods.calendars_list = true;
        calendars.pim_methods.events_in_range = true;
        calendars.pim_methods.event_get = true;
        calendars.pim_methods.event_rsvp = false; // unsupported by this provider

        let caps = build_capabilities(&profile, &[], false, None, Some(&calendars), false);
        assert!(caps.pim_methods.calendars_list);
        assert!(caps.pim_methods.event_get);
        assert!(
            !caps.pim_methods.event_rsvp,
            "an unsupported sub-account flag must not be advertised true",
        );
        // IMAP-owned mail flags are untouched by the calendar merge.
        assert!(caps.pim_methods.message_hydrate);
        assert!(caps.pim_methods.search_messages);
    }

    #[test]
    fn capabilities_send_flag_tracks_submission() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());

        let without = build_capabilities(&profile, &[], false, None, None, false);
        assert!(!without.pim_methods.send_message);
        assert!(!without.pim_methods.draft_send);

        let with = build_capabilities(&profile, &[], false, None, None, true);
        assert!(with.pim_methods.send_message);
        assert!(with.pim_methods.draft_send);
        // Submission does not turn on uploaded-attachment support (A6).
        assert!(!with.pim_methods.attachment_upload);
    }

    #[test]
    fn scheduled_send_flag_stays_false_with_submission_configured() {
        // IMAP relay FUTURERELEASE is per-connection (EHLO at send
        // time), unknown at open. The capability snapshot is the honest
        // `false` even when submission is configured; support is decided
        // at send time by the smtp boundary.
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let with_submission = build_capabilities(&profile, &[], false, None, None, true);
        assert!(!with_submission.pim_methods.scheduled_send);
    }
}
