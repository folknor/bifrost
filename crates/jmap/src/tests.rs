#![cfg(test)]

use serde_json::json;

// ---------------------------------------------------------------------------
// 1. CalendarEvent/ContactCard PatchObject null semantics
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod patch_object_null_semantics {
    use super::*;
    use crate::calendar_event::CalendarEvent;
    use crate::contact_card::ContactCard;
    use crate::core::set::SetObjectCreatable;
    use crate::Set;

    #[test]
    fn calendar_event_calendar_id_false_produces_null() {
        let mut event = CalendarEvent::<Set>::new(Some(0));
        event.calendar_id("cal-1", false);

        let value = serde_json::to_value(&event).unwrap();
        let calendar_ids = value.get("calendarIds").expect("calendarIds missing");
        let entry = calendar_ids.get("cal-1").expect("cal-1 missing");
        assert!(
            entry.is_null(),
            "calendar_id(id, false) must produce null, got {:?}",
            entry
        );
    }

    #[test]
    fn calendar_event_calendar_id_true_produces_true() {
        let mut event = CalendarEvent::<Set>::new(Some(0));
        event.calendar_id("cal-1", true);

        let value = serde_json::to_value(&event).unwrap();
        let calendar_ids = value.get("calendarIds").unwrap();
        let entry = calendar_ids.get("cal-1").unwrap();
        assert_eq!(entry, &json!(true));
    }

    #[test]
    fn contact_card_address_book_id_false_produces_null() {
        let mut card = ContactCard::<Set>::new(Some(0));
        card.address_book_id("ab-1", false);

        let value = serde_json::to_value(&card).unwrap();
        let ab_ids = value
            .get("addressBookIds")
            .expect("addressBookIds missing");
        let entry = ab_ids.get("ab-1").expect("ab-1 missing");
        assert!(
            entry.is_null(),
            "address_book_id(id, false) must produce null, got {:?}",
            entry
        );
    }

    #[test]
    fn contact_card_address_book_id_true_produces_true() {
        let mut card = ContactCard::<Set>::new(Some(0));
        card.address_book_id("ab-1", true);

        let value = serde_json::to_value(&card).unwrap();
        let ab_ids = value.get("addressBookIds").unwrap();
        let entry = ab_ids.get("ab-1").unwrap();
        assert_eq!(entry, &json!(true));
    }
}

// ---------------------------------------------------------------------------
// 2. CalendarEvent nullable getter semantics
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod calendar_event_nullable_getters {
    use crate::calendar_event::CalendarEvent;
    use crate::core::field::Field;
    use crate::Get;

    /// Helper: deserialize a CalendarEvent<Get> from a JSON string.
    fn from_json(s: &str) -> CalendarEvent<Get> {
        serde_json::from_str(s).expect("failed to deserialize CalendarEvent<Get>")
    }

    // -- time_zone --

    #[test]
    fn time_zone_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.time_zone().is_omitted());
    }

    #[test]
    fn time_zone_null_returns_null() {
        let event = from_json(r#"{"timeZone":null}"#);
        assert!(event.time_zone().is_null());
    }

    #[test]
    fn time_zone_present_returns_value() {
        let event = from_json(r#"{"timeZone":"America/New_York"}"#);
        assert_eq!(event.time_zone(), Field::Value("America/New_York"));
    }

    // -- color --

    #[test]
    fn color_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.color().is_omitted());
    }

    #[test]
    fn color_null_returns_null() {
        let event = from_json(r#"{"color":null}"#);
        assert!(event.color().is_null());
    }

    #[test]
    fn color_present_returns_value() {
        let event = from_json(r##"{"color":"#ff0000"}"##);
        assert_eq!(event.color(), Field::Value("#ff0000"));
    }

    // -- locale --

    #[test]
    fn locale_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.locale().is_omitted());
    }

    #[test]
    fn locale_null_returns_null() {
        let event = from_json(r#"{"locale":null}"#);
        assert!(event.locale().is_null());
    }

    #[test]
    fn locale_present_returns_value() {
        let event = from_json(r#"{"locale":"en-US"}"#);
        assert_eq!(event.locale(), Field::Value("en-US"));
    }

    // -- alerts --

    #[test]
    fn alerts_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.alerts().is_omitted());
    }

    #[test]
    fn alerts_null_returns_null() {
        let event = from_json(r#"{"alerts":null}"#);
        assert!(event.alerts().is_null(), "null alerts should give Field::Null");
    }

    #[test]
    fn alerts_present_returns_value() {
        let event = from_json(
            r#"{"alerts":{"a1":{"trigger":{"@type":"OffsetTrigger","offset":"-PT15M"},"action":"display"}}}"#,
        );
        let alerts = event.alerts();
        let map = alerts.as_value().expect("alerts should be Value(map)");
        assert!(map.contains_key("a1"));
    }
}

// ---------------------------------------------------------------------------
// 3. CalendarEvent/ContactCard round-trip with extension properties
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod extension_property_round_trip {
    use super::*;
    use crate::calendar_event::CalendarEvent;
    use crate::contact_card::ContactCard;
    use crate::Get;

    #[test]
    fn calendar_event_preserves_extension_properties() {
        let input = json!({
            "uid": "evt-1",
            "title": "Meeting",
            "example.com:custom-field": "custom-value",
            "vendor.io:priority": 42
        });

        let event: CalendarEvent<Get> =
            serde_json::from_value(input.clone()).expect("deser failed");

        // Verify typed accessors work
        assert_eq!(event.uid(), Some("evt-1"));
        assert_eq!(event.title(), Some("Meeting"));

        // Verify extension properties are accessible
        assert_eq!(
            event.property("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(
            event.property("vendor.io:priority"),
            Some(&json!(42))
        );

        // Reserialize and verify extension properties survive
        let output = serde_json::to_value(&event).unwrap();
        assert_eq!(
            output.get("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(output.get("vendor.io:priority"), Some(&json!(42)));
        assert_eq!(output.get("uid"), Some(&json!("evt-1")));
    }

    #[test]
    fn contact_card_preserves_extension_properties() {
        let input = json!({
            "uid": "card-1",
            "kind": "individual",
            "example.com:department": "Engineering"
        });

        let card: ContactCard<Get> =
            serde_json::from_value(input.clone()).expect("deser failed");

        assert_eq!(card.uid(), Some("card-1"));
        assert_eq!(card.kind(), Some("individual"));
        assert_eq!(
            card.property("example.com:department"),
            Some(&json!("Engineering"))
        );

        let output = serde_json::to_value(&card).unwrap();
        assert_eq!(
            output.get("example.com:department"),
            Some(&json!("Engineering"))
        );
        assert_eq!(output.get("uid"), Some(&json!("card-1")));
    }
}

// ---------------------------------------------------------------------------
// 4. Property enum round-trip
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod property_enum_round_trip {
    use super::*;
    use crate::calendar_event::Property as CEProperty;
    use crate::contact_card::Property as CCProperty;

    /// All known CalendarEvent::Property variants and their wire names.
    fn ce_known_variants() -> Vec<(CEProperty, &'static str)> {
        vec![
            (CEProperty::Id, "id"),
            (CEProperty::Uid, "uid"),
            (CEProperty::CalendarIds, "calendarIds"),
            (CEProperty::IsDraft, "isDraft"),
            (CEProperty::Title, "title"),
            (CEProperty::Description, "description"),
            (CEProperty::DescriptionContentType, "descriptionContentType"),
            (CEProperty::Created, "created"),
            (CEProperty::Updated, "updated"),
            (CEProperty::Start, "start"),
            (CEProperty::Duration, "duration"),
            (CEProperty::TimeZone, "timeZone"),
            (CEProperty::ShowWithoutTime, "showWithoutTime"),
            (CEProperty::Status, "status"),
            (CEProperty::FreeBusyStatus, "freeBusyStatus"),
            (CEProperty::RecurrenceId, "recurrenceId"),
            (CEProperty::RecurrenceIdTimeZone, "recurrenceIdTimeZone"),
            (CEProperty::RecurrenceRules, "recurrenceRules"),
            (CEProperty::RecurrenceOverrides, "recurrenceOverrides"),
            (CEProperty::ExcludedRecurrenceRules, "excludedRecurrenceRules"),
            (CEProperty::Priority, "priority"),
            (CEProperty::Color, "color"),
            (CEProperty::Locale, "locale"),
            (CEProperty::Keywords, "keywords"),
            (CEProperty::Categories, "categories"),
            (CEProperty::ProdId, "prodId"),
            (CEProperty::ReplyTo, "replyTo"),
            (CEProperty::Participants, "participants"),
            (CEProperty::UseDefaultAlerts, "useDefaultAlerts"),
            (CEProperty::Alerts, "alerts"),
            (CEProperty::Locations, "locations"),
            (CEProperty::VirtualLocations, "virtualLocations"),
            (CEProperty::Links, "links"),
            (CEProperty::RelatedTo, "relatedTo"),
            (CEProperty::ExcludedDates, "excludedDates"),
            (CEProperty::Localizations, "localizations"),
            (CEProperty::Method, "method"),
            (CEProperty::Sequence, "sequence"),
        ]
    }

    #[test]
    fn calendar_event_property_display_round_trip() {
        for (prop, wire_name) in ce_known_variants() {
            // Display -> &str -> From<&str>
            let displayed = prop.to_string();
            assert_eq!(displayed, wire_name, "Display mismatch for {:?}", prop);
            let parsed = CEProperty::from(displayed.as_str());
            assert_eq!(parsed, prop, "From<&str> mismatch for {}", wire_name);
        }
    }

    #[test]
    fn calendar_event_property_serde_round_trip() {
        for (prop, wire_name) in ce_known_variants() {
            let serialized = serde_json::to_value(&prop).unwrap();
            assert_eq!(serialized, json!(wire_name));
            let deserialized: CEProperty = serde_json::from_value(serialized).unwrap();
            assert_eq!(deserialized, prop);
        }
    }

    #[test]
    fn calendar_event_property_other_round_trip() {
        let prop = CEProperty::Other("example.com:custom".to_string());
        let displayed = prop.to_string();
        assert_eq!(displayed, "example.com:custom");
        let parsed = CEProperty::from("example.com:custom");
        assert_eq!(parsed, prop);

        let serialized = serde_json::to_value(&prop).unwrap();
        assert_eq!(serialized, json!("example.com:custom"));
        let deserialized: CEProperty = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, prop);
    }

    /// All known ContactCard::Property variants and their wire names.
    fn cc_known_variants() -> Vec<(CCProperty, &'static str)> {
        vec![
            (CCProperty::Id, "id"),
            (CCProperty::Uid, "uid"),
            (CCProperty::AddressBookIds, "addressBookIds"),
            (CCProperty::Kind, "kind"),
            (CCProperty::Name, "name"),
            (CCProperty::Nicknames, "nicknames"),
            (CCProperty::Emails, "emails"),
            (CCProperty::Phones, "phones"),
            (CCProperty::Addresses, "addresses"),
            (CCProperty::Organizations, "organizations"),
            (CCProperty::OnlineServices, "onlineServices"),
            (CCProperty::Notes, "notes"),
            (CCProperty::Media, "media"),
            (CCProperty::Created, "created"),
            (CCProperty::Updated, "updated"),
        ]
    }

    #[test]
    fn contact_card_property_display_round_trip() {
        for (prop, wire_name) in cc_known_variants() {
            let displayed = prop.to_string();
            assert_eq!(displayed, wire_name, "Display mismatch for {:?}", prop);
            let parsed = CCProperty::from(displayed.as_str());
            assert_eq!(parsed, prop, "From<&str> mismatch for {}", wire_name);
        }
    }

    #[test]
    fn contact_card_property_serde_round_trip() {
        for (prop, wire_name) in cc_known_variants() {
            let serialized = serde_json::to_value(&prop).unwrap();
            assert_eq!(serialized, json!(wire_name));
            let deserialized: CCProperty = serde_json::from_value(serialized).unwrap();
            assert_eq!(deserialized, prop);
        }
    }

    #[test]
    fn contact_card_property_other_round_trip() {
        let prop = CCProperty::Other("vendor:x-field".to_string());
        let displayed = prop.to_string();
        assert_eq!(displayed, "vendor:x-field");
        let parsed = CCProperty::from("vendor:x-field");
        assert_eq!(parsed, prop);

        let serialized = serde_json::to_value(&prop).unwrap();
        assert_eq!(serialized, json!("vendor:x-field"));
        let deserialized: CCProperty = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, prop);
    }
}

// ---------------------------------------------------------------------------
// 5. Query filter serialization
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts", feature = "quota"))]
mod query_filter_serialization {
    use super::*;
    use crate::calendar_event::query::Filter as CEFilter;
    use crate::contact_card::query::Filter as CCFilter;
    use crate::quota::query::Filter as QFilter;

    // -- ContactCard filters with slashes in property names --

    #[test]
    fn contact_card_name_given_filter() {
        let filter = CCFilter::name_given("Alice");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/given": "Alice"}));
    }

    #[test]
    fn contact_card_name_surname_filter() {
        let filter = CCFilter::name_surname("Smith");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname": "Smith"}));
    }

    #[test]
    fn contact_card_name_surname2_filter() {
        let filter = CCFilter::name_surname2("Garcia");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname2": "Garcia"}));
    }

    #[test]
    fn contact_card_in_address_book_filter() {
        let filter = CCFilter::in_address_book("ab-123");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inAddressBook": "ab-123"}));
    }

    // -- CalendarEvent filters: inCalendar (singular) vs inCalendars (plural) --

    #[test]
    fn calendar_event_in_calendar_singular() {
        let filter = CEFilter::in_calendar("cal-1");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendar": "cal-1"}));
    }

    #[test]
    fn calendar_event_in_calendars_plural() {
        let filter = CEFilter::in_calendars(["cal-1", "cal-2"]);
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendars": ["cal-1", "cal-2"]}));
    }

    #[test]
    fn calendar_event_text_filter() {
        let filter = CEFilter::text("meeting");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"text": "meeting"}));
    }

    #[test]
    fn calendar_event_uid_filter() {
        let filter = CEFilter::uid("uid-abc");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"uid": "uid-abc"}));
    }

    // -- Quota filters --

    #[test]
    fn quota_name_filter() {
        let filter = QFilter::name("Storage");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name": "Storage"}));
    }

    #[test]
    fn quota_scope_filter() {
        let filter = QFilter::scope("account");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"scope": "account"}));
    }

    #[test]
    fn quota_resource_type_filter() {
        let filter = QFilter::resource_type("octets");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"resourceType": "octets"}));
    }
}

// ---------------------------------------------------------------------------
// 6. Calendar Option<Option<T>> serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod calendar_option_option_serialization {
    use super::*;
    use crate::calendar::Calendar;
    use crate::core::set::SetObjectCreatable;
    use crate::Set;

    #[test]
    fn calendar_description_none_serializes_as_null() {
        let mut cal = Calendar::<Set>::new(Some(0));
        cal.name("Test Calendar");
        cal.description(None::<String>);

        let value = serde_json::to_value(&cal).unwrap();
        // description is set to Some(None), so it serializes as null
        assert!(
            value.get("description").is_some(),
            "description should be present in JSON"
        );
        assert!(
            value.get("description").unwrap().is_null(),
            "description should be null, got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_unset_is_absent() {
        let mut cal = Calendar::<Set>::new(Some(0));
        cal.name("Test Calendar");
        // Do NOT call cal.description() — leave it as outer None

        let value = serde_json::to_value(&cal).unwrap();
        assert!(
            value.get("description").is_none(),
            "unset description should be absent from JSON, but got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_some_serializes_as_string() {
        let mut cal = Calendar::<Set>::new(Some(0));
        cal.name("Test Calendar");
        cal.description(Some("A nice calendar"));

        let value = serde_json::to_value(&cal).unwrap();
        assert_eq!(
            value.get("description"),
            Some(&json!("A nice calendar"))
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Blob DataSource serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "blob")]
mod blob_data_source_serialization {
    use super::*;
    use crate::blob::manage::{
        DataSource, DataSourceBase64, DataSourceBlob, DataSourceText,
    };

    #[test]
    fn data_source_text_serialization() {
        let src = DataSource::Text(DataSourceText {
            value: "Hello, world!".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asText": "Hello, world!"}));
    }

    #[test]
    fn data_source_base64_serialization() {
        let src = DataSource::Base64(DataSourceBase64 {
            value: "SGVsbG8=".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asBase64": "SGVsbG8="}));
    }

    #[test]
    fn data_source_blob_serialization() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-123".into(),
            offset: Some(10),
            length: Some(100),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(
            value,
            json!({"blobId": "blob-123", "offset": 10, "length": 100})
        );
    }

    #[test]
    fn data_source_blob_without_range() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-456".into(),
            offset: None,
            length: None,
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"blobId": "blob-456"}));
        // offset and length should be absent
        assert!(value.get("offset").is_none());
        assert!(value.get("length").is_none());
    }
}

// ---------------------------------------------------------------------------
// 8. Session capabilities deserialization
// ---------------------------------------------------------------------------

mod session_capabilities_deserialization {
    use super::*;
    use crate::core::session::{Capabilities, Session};

    fn sample_session_json() -> serde_json::Value {
        json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 50000000,
                    "maxConcurrentUpload": 4,
                    "maxSizeRequest": 10000000,
                    "maxConcurrentRequests": 8,
                    "maxCallsInRequest": 16,
                    "maxObjectsInGet": 500,
                    "maxObjectsInSet": 500,
                    "collationAlgorithms": ["i;ascii-casemap"]
                },
                "urn:ietf:params:jmap:mail": {
                    "maxMailboxesPerEmail": null,
                    "maxMailboxDepth": 10,
                    "maxSizeMailboxName": 200,
                    "maxSizeAttachmentsPerEmail": 50000000,
                    "emailQuerySortOptions": ["receivedAt", "from", "subject"],
                    "mayCreateTopLevelMailbox": true
                },
                "urn:ietf:params:jmap:quota": {},
                "urn:ietf:params:jmap:blob": {
                    "maxSizeBlobSet": 100000000,
                    "supportedDigestAlgorithms": ["sha", "sha-256"],
                    "supportedTypeNames": ["Email", "CalendarEvent"]
                },
                "urn:ietf:params:jmap:calendars": {
                    "mayCreateCalendar": true,
                    "maxCalendarsPerEvent": null
                },
                "urn:ietf:params:jmap:contacts": {
                    "mayCreateAddressBook": true,
                    "maxAddressBooksPerCard": null
                },
                "urn:ietf:params:jmap:principals": {
                    "currentUserPrincipalId": "user-1",
                    "accountIdForPrincipal": "acct-1"
                },
                "urn:vendor:custom": {
                    "someField": "someValue"
                }
            },
            "accounts": {
                "acct-1": {
                    "name": "John Doe",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:core": {},
                        "urn:ietf:params:jmap:mail": {
                            "maxMailboxesPerEmail": null,
                            "maxMailboxDepth": 10,
                            "maxSizeMailboxName": 200,
                            "maxSizeAttachmentsPerEmail": 50000000,
                            "emailQuerySortOptions": [],
                            "mayCreateTopLevelMailbox": true
                        }
                    }
                }
            },
            "primaryAccounts": {
                "urn:ietf:params:jmap:mail": "acct-1"
            },
            "username": "john@example.org",
            "apiUrl": "https://jmap.example.org/api/",
            "downloadUrl": "https://jmap.example.org/download/{accountId}/{blobId}/{name}",
            "uploadUrl": "https://jmap.example.org/upload/{accountId}/",
            "eventSourceUrl": "https://jmap.example.org/eventsource/",
            "state": "s0"
        })
    }

    #[test]
    fn session_deserializes_all_capability_types() {
        let session: Session =
            serde_json::from_value(sample_session_json()).expect("session deser failed");

        // core
        let core = session.core_capabilities().expect("core missing");
        assert_eq!(core.max_size_upload(), 50_000_000);
        assert_eq!(core.max_calls_in_request(), 16);
        assert_eq!(core.max_objects_in_get(), 500);
        assert_eq!(core.collation_algorithms(), &["i;ascii-casemap"]);

        #[cfg(feature = "mail")]
        {
            let mail = session.mail_capabilities().expect("mail missing");
            assert_eq!(mail.max_mailbox_depth(), 10);
        }

        #[cfg(feature = "quota")]
        assert!(session.quota_capabilities().is_some());

        #[cfg(feature = "blob")]
        {
            let blob = session.blob_capabilities().expect("blob missing");
            assert_eq!(blob.max_size_blob_set(), Some(100_000_000));
            assert_eq!(
                blob.supported_digest_algorithms(),
                &["sha", "sha-256"]
            );
        }

        #[cfg(feature = "calendars")]
        {
            let cals = session
                .calendars_capabilities()
                .expect("calendars missing");
            assert!(cals.may_create_calendar());
            assert_eq!(cals.max_calendars_per_event(), None);
        }

        #[cfg(feature = "contacts")]
        {
            let contacts = session
                .contacts_capabilities()
                .expect("contacts missing");
            assert!(contacts.may_create_address_book());
        }

        // principals
        let principals = session
            .principals_capabilities()
            .expect("principals missing");
        assert_eq!(
            principals.current_user_principal_id(),
            Some("user-1")
        );
        assert_eq!(
            principals.account_id_for_principal(),
            Some("acct-1")
        );

        // unknown vendor capability -> Other
        let vendor = session
            .capability("urn:vendor:custom")
            .expect("vendor cap missing");
        match vendor {
            Capabilities::Other(v) => {
                assert_eq!(v.get("someField"), Some(&json!("someValue")));
            }
            other => panic!(
                "expected Capabilities::Other for vendor cap, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn session_basic_fields() {
        let session: Session =
            serde_json::from_value(sample_session_json()).expect("session deser failed");

        assert_eq!(session.username(), "john@example.org");
        assert_eq!(session.api_url(), "https://jmap.example.org/api/");
        assert_eq!(session.state(), "s0");

        let account = session.account("acct-1").expect("account missing");
        assert_eq!(account.name(), "John Doe");
        assert!(account.is_personal());
        assert!(!account.is_read_only());
    }
}

// ---------------------------------------------------------------------------
// 9. AlertTrigger deserialization
// ---------------------------------------------------------------------------
#[cfg(feature = "calendars")]

mod alert_trigger_deserialization {
    use super::*;
    use crate::calendar_event::{AlertTrigger, RelativeTo};

    #[test]
    fn offset_trigger_deserializes() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"-PT15M","relativeTo":"start"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "-PT15M");
                assert_eq!(relative_to, Some(RelativeTo::Start));
            }
            other => panic!("expected OffsetTrigger, got {:?}", other),
        }
    }

    #[test]
    fn offset_trigger_without_relative_to() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"PT0S"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "PT0S");
                assert_eq!(relative_to, None);
            }
            other => panic!("expected OffsetTrigger, got {:?}", other),
        }
    }

    #[test]
    fn absolute_trigger_deserializes() {
        let json_str =
            r#"{"@type":"AbsoluteTrigger","when":"2025-06-15T09:00:00Z"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::AbsoluteTrigger { when } => {
                assert_eq!(when, "2025-06-15T09:00:00Z");
            }
            other => panic!("expected AbsoluteTrigger, got {:?}", other),
        }
    }

    #[test]
    fn unknown_trigger_type_deserializes_as_unknown() {
        // #[serde(other)] catches any unrecognized @type value.
        let json_str = r#"{"@type":"FutureTriggerType","foo":"bar"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        assert!(
            matches!(trigger, AlertTrigger::Unknown),
            "expected Unknown, got {:?}",
            trigger
        );
    }

    #[test]
    fn offset_trigger_serializes() {
        let trigger = AlertTrigger::OffsetTrigger {
            offset: "-PT10M".to_string(),
            relative_to: Some(RelativeTo::End),
        };
        let value = serde_json::to_value(&trigger).unwrap();
        assert_eq!(value.get("@type"), Some(&json!("OffsetTrigger")));
        assert_eq!(value.get("offset"), Some(&json!("-PT10M")));
        assert_eq!(value.get("relativeTo"), Some(&json!("end")));
    }
}

// ---------------------------------------------------------------------------
// 10. Blob/get request serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "blob")]
mod blob_get_request_serialization {
    use super::*;
    use crate::blob::manage::BlobGetRequest;

    #[test]
    fn blob_get_request_basic_serialization() {
        let mut req = BlobGetRequest::new("acct-1");
        req.ids(["blob-1", "blob-2"]);
        req.properties(["data:asText", "size"]);

        let value = serde_json::to_value(&req).unwrap();

        // accountId at top level
        assert_eq!(value.get("accountId"), Some(&json!("acct-1")));

        // ids as a flat array
        let ids = value.get("ids").expect("ids missing");
        assert!(ids.is_array());
        let ids_arr = ids.as_array().unwrap();
        assert_eq!(ids_arr.len(), 2);
        assert!(ids_arr.contains(&json!("blob-1")));
        assert!(ids_arr.contains(&json!("blob-2")));

        // properties at top level
        let props = value.get("properties").expect("properties missing");
        assert!(props.is_array());
        let props_arr = props.as_array().unwrap();
        assert!(props_arr.contains(&json!("data:asText")));
        assert!(props_arr.contains(&json!("size")));
    }

    #[test]
    fn blob_get_request_with_offset_and_length() {
        let mut req = BlobGetRequest::new("acct-1");
        req.ids(["blob-1"]);
        req.offset(100);
        req.length(500);

        let value = serde_json::to_value(&req).unwrap();
        assert_eq!(value.get("offset"), Some(&json!(100)));
        assert_eq!(value.get("length"), Some(&json!(500)));
    }

    #[test]
    fn blob_get_request_without_optional_fields() {
        let mut req = BlobGetRequest::new("acct-1");
        req.ids(["blob-1"]);

        let value = serde_json::to_value(&req).unwrap();
        // properties, offset, length should be absent
        assert!(
            value.get("properties").is_none(),
            "unset properties should be absent"
        );
        assert!(
            value.get("offset").is_none(),
            "unset offset should be absent"
        );
        assert!(
            value.get("length").is_none(),
            "unset length should be absent"
        );
    }
}

// ---------------------------------------------------------------------------
// ShareNotification (RFC 9670)
// ---------------------------------------------------------------------------

mod share_notification_serde {
    use super::*;
    use crate::share_notification::ShareNotification;
    use crate::Get;

    fn from_json(s: &str) -> ShareNotification<Get> {
        serde_json::from_str(s).expect("failed to deserialize ShareNotification<Get>")
    }

    #[test]
    fn full_notification_round_trip() {
        let input = json!({
            "id": "notif-1",
            "created": "2024-11-15T10:30:00Z",
            "changedBy": {
                "name": "Alice",
                "email": "alice@example.com",
                "principalId": "p-alice"
            },
            "objectType": "Calendar",
            "objectAccountId": "acct-bob",
            "objectId": "cal-team",
            "oldRights": null,
            "newRights": {
                "mayReadFreeBusy": true,
                "mayReadItems": true,
                "mayWriteAll": false
            },
            "name": "Team Calendar"
        });

        let notif: ShareNotification<Get> = serde_json::from_value(input).unwrap();
        assert_eq!(notif.id(), Some("notif-1"));
        assert_eq!(notif.created(), Some("2024-11-15T10:30:00Z"));
        assert_eq!(notif.object_type(), Some("Calendar"));
        assert_eq!(notif.object_account_id(), Some("acct-bob"));
        assert_eq!(notif.object_id(), Some("cal-team"));
        assert_eq!(notif.name(), Some("Team Calendar"));
        assert!(notif.old_rights().is_none());
        let new_rights = notif.new_rights().unwrap();
        assert_eq!(new_rights.get("mayReadFreeBusy"), Some(&true));
        assert_eq!(new_rights.get("mayWriteAll"), Some(&false));

        let changed_by = notif.changed_by().unwrap();
        assert_eq!(changed_by.name(), Some("Alice"));
        assert_eq!(changed_by.email(), Some("alice@example.com"));
        assert_eq!(changed_by.principal_id(), Some("p-alice"));
    }

    #[test]
    fn minimal_notification_deserializes() {
        let notif = from_json(r#"{"id":"n1"}"#);
        assert_eq!(notif.id(), Some("n1"));
        assert!(notif.created().is_none());
        assert!(notif.changed_by().is_none());
        assert!(notif.object_type().is_none());
        assert!(notif.old_rights().is_none());
        assert!(notif.new_rights().is_none());
    }

    #[test]
    fn property_display() {
        use crate::share_notification::Property;
        assert_eq!(Property::ObjectType.to_string(), "objectType");
        assert_eq!(Property::ChangedBy.to_string(), "changedBy");
        assert_eq!(Property::OldRights.to_string(), "oldRights");
        assert_eq!(Property::NewRights.to_string(), "newRights");
        assert_eq!(Property::ObjectAccountId.to_string(), "objectAccountId");
    }

    #[test]
    fn filter_serialization() {
        use crate::share_notification::query::Filter;

        let f = Filter::after("2024-01-01T00:00:00Z");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"after": "2024-01-01T00:00:00Z"}));

        let f = Filter::object_type("Calendar");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectType": "Calendar"}));

        let f = Filter::object_account_id("acct-1");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectAccountId": "acct-1"}));
    }
}

// ---------------------------------------------------------------------------
// Principal RFC 9670 additions
// ---------------------------------------------------------------------------

mod principal_rfc9670 {
    use super::*;
    use crate::principal::Principal;
    use crate::Get;

    #[test]
    fn principal_with_accounts_deserializes() {
        let input = json!({
            "id": "p-alice",
            "type": "individual",
            "name": "Alice",
            "email": "alice@example.com",
            "capabilities": {
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:calendars": {"mayCreateCalendar": true}
            },
            "accounts": {
                "acct-1": {
                    "name": "alice@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:mail": {}
                    }
                },
                "acct-2": {
                    "name": "shared",
                    "isPersonal": false,
                    "isReadOnly": true,
                    "accountCapabilities": {}
                }
            }
        });

        let principal: Principal<Get> = serde_json::from_value(input).unwrap();
        assert_eq!(principal.id(), Some("p-alice"));
        assert_eq!(principal.name(), Some("Alice"));

        let caps = principal.capabilities().unwrap();
        assert!(caps.contains_key("urn:ietf:params:jmap:mail"));
        assert!(caps.contains_key("urn:ietf:params:jmap:calendars"));

        let accounts = principal.accounts().unwrap();
        assert_eq!(accounts.len(), 2);

        let acct1 = &accounts["acct-1"];
        assert_eq!(acct1.name(), Some("alice@example.com"));
        assert!(acct1.is_personal());
        assert!(!acct1.is_read_only());
        assert!(acct1.account_capabilities().contains_key("urn:ietf:params:jmap:mail"));

        let acct2 = &accounts["acct-2"];
        assert_eq!(acct2.name(), Some("shared"));
        assert!(!acct2.is_personal());
        assert!(acct2.is_read_only());
    }

    #[test]
    fn principal_without_accounts_deserializes() {
        let input = json!({
            "id": "p-bob",
            "type": "individual",
            "name": "Bob"
        });

        let principal: Principal<Get> = serde_json::from_value(input).unwrap();
        assert_eq!(principal.id(), Some("p-bob"));
        assert!(principal.accounts().is_none());
        assert!(principal.capabilities().is_none());
    }

    #[test]
    fn principal_account_ids_filter_serialization() {
        use crate::principal::query::Filter;
        let f = Filter::account_ids(["acct-1", "acct-2"]);
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"accountIds": ["acct-1", "acct-2"]}));
    }
}

// ---------------------------------------------------------------------------
// PrincipalsOwner capability
// ---------------------------------------------------------------------------

mod principals_owner_capability {
    use super::*;

    #[test]
    fn session_with_principals_owner() {
        let session_json = json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 50000000,
                    "maxConcurrentUpload": 4,
                    "maxSizeRequest": 10000000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 16,
                    "maxObjectsInGet": 500,
                    "maxObjectsInSet": 500,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:principals": {},
                "urn:ietf:params:jmap:principals:owner": {
                    "accountIdForPrincipal": "acct-principals",
                    "principalId": "p-owner"
                }
            },
            "accounts": {
                "acct-1": {
                    "name": "user@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:principals": {
                            "currentUserPrincipalId": "p-user"
                        },
                        "urn:ietf:params:jmap:principals:owner": {
                            "accountIdForPrincipal": "acct-principals",
                            "principalId": "p-user"
                        }
                    }
                }
            },
            "primaryAccounts": {
                "urn:ietf:params:jmap:principals": "acct-1"
            },
            "username": "user@example.com",
            "apiUrl": "https://example.com/jmap/",
            "downloadUrl": "https://example.com/jmap/download/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.com/jmap/upload/{accountId}/",
            "eventSourceUrl": "https://example.com/jmap/eventsource/?types={types}&closeafter={closeafter}&ping={ping}",
            "state": "abc123"
        });

        let session: crate::core::session::Session =
            serde_json::from_value(session_json).unwrap();

        // Session-level principals capability (empty object → all fields None)
        let principals = session.principals_capabilities().unwrap();
        assert!(principals.current_user_principal_id().is_none());

        // Session-level principals:owner capability
        let owner = session.principals_owner_capabilities().unwrap();
        assert_eq!(owner.account_id_for_principal(), Some("acct-principals"));
        assert_eq!(owner.principal_id(), Some("p-owner"));

        // Account-level principals capability
        let account = session.account("acct-1").unwrap();
        let acct_principals = account.capability("urn:ietf:params:jmap:principals").unwrap();
        match acct_principals {
            crate::core::session::Capabilities::Principals(c) => {
                assert_eq!(c.current_user_principal_id(), Some("p-user"));
            }
            _ => panic!("expected Principals variant"),
        }

        // Account-level principals:owner capability
        let acct_owner = account.capability("urn:ietf:params:jmap:principals:owner").unwrap();
        match acct_owner {
            crate::core::session::Capabilities::PrincipalsOwner(c) => {
                assert_eq!(c.account_id_for_principal(), Some("acct-principals"));
                assert_eq!(c.principal_id(), Some("p-user"));
            }
            _ => panic!("expected PrincipalsOwner variant"),
        }
    }
}
