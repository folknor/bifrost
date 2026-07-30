use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AddressBook, ContactAddress, ContactCard,
    ContactCorpus, ContactCreate, ContactEmail, ContactId, ContactOrganization, ContactPatch,
    ContactPhone, ContactProvenance, ContactSearchRequest, Page, ProtocolKind,
};
use bifrost_types::{AddressBookId as SharedAddressBookId, DiagnosticText};
use serde_json::{Map, Value, json};

use crate::account::Account as JmapProtoAccount;
use crate::address_book::{AddressBookGet, AddressBookId as JmapAddressBookId};
use crate::contact_card::query::Filter as ContactFilter;
use crate::contact_card::{
    ContactCard as JmapContactCard, ContactCardCreate, ContactCardGet, ContactCardId,
    ContactCardPatch, ContactCardQuery, ContactCardSet,
};
use crate::core::SetCreate;
use crate::transport_reqwest::ReqwestTransport;

type ContactAccount = JmapProtoAccount<ReqwestTransport>;

const PAGE_LIMIT: usize = 100;

pub(crate) fn address_books_list(
    contacts: Option<ContactAccount>,
) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::AddressBooksList)?;
        let response = contacts
            .call(AddressBookGet::new())
            .await
            .map_err(to_acct_err(AccountOperation::AddressBooksList))?;
        Ok(response
            .into_list()
            .into_iter()
            .map(address_book_from_jmap)
            .collect())
    })
}

pub(crate) fn list(
    contacts: Option<ContactAccount>,
    address_book: Option<SharedAddressBookId>,
    page_cursor: Option<Vec<u8>>,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactsList)?;
        let position = decode_position(page_cursor, AccountOperation::ContactsList)?;
        let mut query = ContactCardQuery::new()
            .position(position)
            .limit(PAGE_LIMIT)
            .calculate_total(true);
        if let Some(book) = address_book {
            query = query.filter(ContactFilter::in_address_book(JmapAddressBookId::new(
                book.0,
            )));
        }
        let query_response = contacts
            .call(query)
            .await
            .map_err(to_acct_err(AccountOperation::ContactsList))?;
        let total = query_response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::ContactsList))
            .transpose()?;
        let next_cursor = next_cursor(
            query_response.position(),
            query_response.ids().len(),
            query_response.total(),
        );
        let hydrated = get_cards(
            &contacts,
            query_response.into_ids(),
            AccountOperation::ContactsList,
        )
        .await?;
        Ok(Page {
            items: hydrated.cards,
            next_cursor,
            estimated_total: total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn get(
    contacts: Option<ContactAccount>,
    contact: ContactId,
) -> AccountFuture<Result<ContactCard, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactGet)?;
        let hydrated = get_cards(
            &contacts,
            vec![ContactCardId::new(contact.0)],
            AccountOperation::ContactGet,
        )
        .await?;
        hydrated
            .cards
            .into_iter()
            .next()
            .ok_or_else(|| unsupported(AccountOperation::ContactGet, "JMAP contact was not found"))
    })
}

pub(crate) fn create(
    contacts: Option<ContactAccount>,
    contact: ContactCreate,
) -> AccountFuture<Result<ContactId, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactCreate)?;
        let mut set = ContactCardSet::new();
        let create_id = set.create_item(jmap_create_from_contact(&contact));
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactCreate))?;
        let card = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::ContactCreate))?;
        let id = card
            .id()
            .map(ContactCardId::into_string)
            .unwrap_or_default();
        Ok(ContactId(id))
    })
}

pub(crate) fn update(
    contacts: Option<ContactAccount>,
    contact: ContactId,
    patch: ContactPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactUpdate)?;
        if patch.photo.is_some() {
            return Err(unsupported(
                AccountOperation::ContactUpdate,
                "JMAP contact raw photo updates are unsupported; use photo_url media instead",
            ));
        }
        let id = ContactCardId::new(contact.0);
        let current_address_book = if patch.address_book_id.is_some() {
            get_cards(&contacts, vec![id.clone()], AccountOperation::ContactUpdate)
                .await?
                .cards
                .into_iter()
                .next()
                .and_then(|card| card.address_book_id)
        } else {
            None
        };
        let mut set = ContactCardSet::new();
        set.update_item(
            id.clone(),
            jmap_patch_from_contact_patch(&patch, current_address_book.as_ref()),
        );
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactUpdate))?;
        response
            .updated(&id)
            .map_err(to_acct_err(AccountOperation::ContactUpdate))?;
        Ok(())
    })
}

pub(crate) fn delete(
    contacts: Option<ContactAccount>,
    contact: ContactId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactDelete)?;
        let id = ContactCardId::new(contact.0);
        let set = ContactCardSet::new().destroy([id.clone()]);
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactDelete))?;
        response
            .destroyed(&id)
            .map_err(to_acct_err(AccountOperation::ContactDelete))
    })
}

pub(crate) fn search(
    contacts: Option<ContactAccount>,
    request: ContactSearchRequest,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactSearch)?;
        let position = decode_position(request.page_cursor, AccountOperation::ContactSearch)?;
        let limit = request
            .limit
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(PAGE_LIMIT);
        let query = ContactCardQuery::new()
            .position(position)
            .limit(limit)
            .filter(ContactFilter::text(request.query))
            .calculate_total(true);
        let query_response = contacts
            .call(query)
            .await
            .map_err(to_acct_err(AccountOperation::ContactSearch))?;
        let total = query_response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::ContactSearch))
            .transpose()?;
        let next_cursor = next_cursor(
            query_response.position(),
            query_response.ids().len(),
            query_response.total(),
        );
        let hydrated = get_cards(
            &contacts,
            query_response.into_ids(),
            AccountOperation::ContactSearch,
        )
        .await?;
        Ok(Page {
            items: hydrated.cards,
            next_cursor,
            estimated_total: total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// A hydrated page of cards plus the native ids the server reported as
/// `notFound`. An id that the query returned but `ContactCard/get` could
/// not materialize is a transient per-resource hydration failure, not a
/// deletion; it rides `Page::failed_ids` so the consumer preserves the
/// row rather than destroying it.
struct HydratedCards {
    cards: Vec<ContactCard>,
    failed_ids: Vec<String>,
}

async fn get_cards(
    contacts: &ContactAccount,
    ids: Vec<ContactCardId>,
    operation: AccountOperation,
) -> Result<HydratedCards, AccountError> {
    if ids.is_empty() {
        return Ok(HydratedCards {
            cards: Vec::new(),
            failed_ids: Vec::new(),
        });
    }
    let response = contacts
        .call(ContactCardGet::new().ids(ids))
        .await
        .map_err(to_acct_err(operation))?;
    let failed_ids = response
        .not_found()
        .iter()
        .cloned()
        .map(ContactCardId::into_string)
        .collect();
    let cards = response
        .into_list()
        .into_iter()
        .map(contact_from_jmap)
        .collect();
    Ok(HydratedCards { cards, failed_ids })
}

fn address_book_from_jmap(book: crate::address_book::AddressBook) -> AddressBook {
    let native = book
        .id
        .map(JmapAddressBookId::into_string)
        .unwrap_or_default();
    let rights = book.my_rights;
    let can_write = address_book_can_write(rights.as_ref());
    AddressBook {
        id: SharedAddressBookId(native.clone()),
        native_id: native.clone(),
        name: book.name.unwrap_or_else(|| "Address Book".to_string()),
        provenance: ContactProvenance {
            provider: ProtocolKind::Jmap,
            native,
            address_book_native: None,
        },
        corpus: ContactCorpus::Main,
        is_default: book.is_default.unwrap_or(false),
        can_create_contacts: can_write,
        can_update_contacts: can_write,
        can_delete_contacts: address_book_can_delete(rights.as_ref()),
    }
}

fn address_book_can_write(rights: Option<&crate::address_book::AddressBookRights>) -> bool {
    rights.and_then(|rights| rights.may_write).unwrap_or(false)
}

fn address_book_can_delete(rights: Option<&crate::address_book::AddressBookRights>) -> bool {
    rights.and_then(|rights| rights.may_delete).unwrap_or(false)
}

fn contact_from_jmap(card: JmapContactCard) -> ContactCard {
    let native = card
        .id()
        .map(ContactCardId::into_string)
        .unwrap_or_default();
    let address_book_id = card
        .address_book_ids()
        .and_then(|ids| ids.keys().next().cloned())
        .map(SharedAddressBookId);
    ContactCard {
        id: ContactId(native.clone()),
        address_book_id: address_book_id.clone(),
        native_id: native.clone(),
        etag: None,
        provenance: ContactProvenance {
            provider: ProtocolKind::Jmap,
            native,
            address_book_native: address_book_id.map(|id| id.0),
        },
        // JMAP has no auto-collected corpus; every card is personal.
        corpus: ContactCorpus::Main,
        display_name: display_name(card.name()),
        emails: emails(card.emails()),
        phones: phones(card.phones()),
        organizations: organizations(card.organizations()),
        addresses: addresses(card.addresses()),
        notes: notes(card.notes()),
        photo_url: photo_url(card.media()),
        photo: None,
    }
}

fn jmap_create_from_contact(contact: &ContactCreate) -> ContactCardCreate {
    let mut create = ContactCardCreate::new(None);
    write_contact_create(&mut create.properties, contact);
    create
}

fn jmap_patch_from_contact_patch(
    patch: &ContactPatch,
    current_address_book: Option<&SharedAddressBookId>,
) -> ContactCardPatch {
    let mut out = ContactCardPatch::default();
    if let Some(address_book_id) = &patch.address_book_id {
        if let Some(current) = current_address_book
            && current.0 != address_book_id.0
        {
            out.address_book_id(JmapAddressBookId::new(current.0.clone()), false);
        }
        out.address_book_id(JmapAddressBookId::new(address_book_id.0.clone()), true);
    }
    if let Some(display_name) = &patch.display_name {
        match display_name {
            Some(name) => {
                out.name(name_object(name));
            }
            None => {
                out.set_property("name", Value::Null);
            }
        }
    }
    if let Some(emails) = &patch.emails {
        out.emails(email_object(emails));
    }
    if let Some(phones) = &patch.phones {
        out.phones(phone_object(phones));
    }
    if let Some(orgs) = &patch.organizations {
        out.organizations(organization_object(orgs));
    }
    if let Some(addresses) = &patch.addresses {
        out.addresses(address_object(addresses));
    }
    if let Some(notes) = &patch.notes {
        match notes {
            Some(notes) => out.notes(notes_object(notes)),
            None => out.set_property("notes", Value::Null),
        };
    }
    if let Some(photo_url) = &patch.photo_url {
        match photo_url {
            Some(url) => {
                let mut media = Map::new();
                media.insert("photo".to_string(), photo_media_object(url));
                out.set_property("media", Value::Object(media));
            }
            None => {
                out.set_property("media", Value::Null);
            }
        }
    }
    out
}

fn write_contact_create(target: &mut Map<String, Value>, contact: &ContactCreate) {
    target.insert("@type".to_string(), Value::String("Card".to_string()));
    target.insert("kind".to_string(), Value::String("individual".to_string()));
    if let Some(address_book_id) = &contact.address_book_id {
        target.insert(
            "addressBookIds".to_string(),
            json!({ address_book_id.0.clone(): true }),
        );
    }
    if let Some(display_name) = &contact.display_name {
        target.insert("name".to_string(), Value::Object(name_object(display_name)));
    }
    if !contact.emails.is_empty() {
        target.insert(
            "emails".to_string(),
            Value::Object(email_object(&contact.emails)),
        );
    }
    if !contact.phones.is_empty() {
        target.insert(
            "phones".to_string(),
            Value::Object(phone_object(&contact.phones)),
        );
    }
    if !contact.organizations.is_empty() {
        target.insert(
            "organizations".to_string(),
            Value::Object(organization_object(&contact.organizations)),
        );
    }
    if !contact.addresses.is_empty() {
        target.insert(
            "addresses".to_string(),
            Value::Object(address_object(&contact.addresses)),
        );
    }
    if let Some(notes) = &contact.notes {
        target.insert("notes".to_string(), Value::Object(notes_object(notes)));
    }
    if let Some(url) = &contact.photo_url {
        let mut media = Map::new();
        media.insert("photo".to_string(), photo_media_object(url));
        target.insert("media".to_string(), Value::Object(media));
    }
}

/// JSContact (RFC 9610) media resource for a photo. `kind` is the resource
/// role (`photo`), not a URI marker; the read path filters on it, so a wrong
/// value reads back as no photo.
fn photo_media_object(url: &str) -> Value {
    json!({"@type": "Media", "kind": "photo", "uri": url})
}

fn name_object(display_name: &str) -> Map<String, Value> {
    let mut name = Map::new();
    name.insert("@type".to_string(), json!("Name"));
    name.insert("full".to_string(), Value::String(display_name.to_string()));
    name
}

fn email_object(emails: &[ContactEmail]) -> Map<String, Value> {
    emails
        .iter()
        .enumerate()
        .map(|(idx, email)| {
            (
                format!("e{idx}"),
                json!({
                    "@type": "EmailAddress",
                    "address": email.value,
                    "contexts": context_object(email.kind.as_deref()),
                    "pref": email.is_primary.then_some(1),
                }),
            )
        })
        .collect()
}

fn phone_object(phones: &[ContactPhone]) -> Map<String, Value> {
    phones
        .iter()
        .enumerate()
        .map(|(idx, phone)| {
            let (contexts, features) = phone_kind_fields(phone.kind.as_deref());
            (
                format!("p{idx}"),
                json!({
                    "@type": "Phone",
                    "number": phone.value,
                    "contexts": contexts,
                    "features": features,
                    "pref": phone.is_primary.then_some(1),
                }),
            )
        })
        .collect()
}

fn organization_object(orgs: &[ContactOrganization]) -> Map<String, Value> {
    orgs.iter()
        .enumerate()
        .map(|(idx, org)| {
            (
                format!("o{idx}"),
                json!({
                    "@type": "Organization",
                    "name": org.name,
                    "title": org.title,
                }),
            )
        })
        .collect()
}

fn address_object(addresses: &[ContactAddress]) -> Map<String, Value> {
    addresses
        .iter()
        .enumerate()
        .map(|(idx, address)| {
            (
                format!("a{idx}"),
                json!({
                    "@type": "Address",
                    "contexts": context_object(address.kind.as_deref()),
                    "pref": address.is_primary.then_some(1),
                    "full": address.formatted.clone(),
                    "street": address.street.join("\n"),
                    "locality": address.locality.clone(),
                    "region": address.region.clone(),
                    "postcode": address.postal_code.clone(),
                    "country": address.country.clone(),
                }),
            )
        })
        .collect()
}

fn notes_object(notes: &str) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("n0".to_string(), json!({"@type": "Note", "note": notes}));
    out
}

fn context_object(kind: Option<&str>) -> Value {
    match kind {
        Some(kind) => json!({ kind: true }),
        None => Value::Null,
    }
}

fn phone_kind_fields(kind: Option<&str>) -> (Value, Value) {
    match kind {
        Some("home" | "work") => (context_object(kind), Value::Null),
        Some(kind) => (Value::Null, json!({ kind: true })),
        None => (Value::Null, Value::Null),
    }
}

fn display_name(name: Option<&Map<String, Value>>) -> Option<String> {
    let name = name?;
    name.get("full")
        .and_then(Value::as_str)
        .or_else(|| name.get("given").and_then(Value::as_str))
        .map(ToString::to_string)
}

fn emails(values: Option<&Map<String, Value>>) -> Vec<ContactEmail> {
    values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(ContactEmail {
                value: object
                    .get("address")
                    .or_else(|| object.get("email"))
                    .and_then(Value::as_str)?
                    .to_string(),
                kind: first_context(object.get("contexts")),
                is_primary: object.get("pref").and_then(Value::as_i64).unwrap_or(0) == 1,
            })
        })
        .collect()
}

fn phones(values: Option<&Map<String, Value>>) -> Vec<ContactPhone> {
    values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(ContactPhone {
                value: object
                    .get("number")
                    .or_else(|| object.get("phone"))
                    .and_then(Value::as_str)?
                    .to_string(),
                kind: phone_kind(object),
                is_primary: object.get("pref").and_then(Value::as_i64).unwrap_or(0) == 1,
            })
        })
        .collect()
}

fn organizations(values: Option<&Map<String, Value>>) -> Vec<ContactOrganization> {
    values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(ContactOrganization {
                name: object.get("name").and_then(Value::as_str)?.to_string(),
                title: object
                    .get("title")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            })
        })
        .collect()
}

fn addresses(values: Option<&Map<String, Value>>) -> Vec<ContactAddress> {
    values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            let street = object
                .get("street")
                .and_then(Value::as_str)
                .map(|street| {
                    street
                        .lines()
                        .filter(|line| !line.is_empty())
                        .map(ToString::to_string)
                        .collect()
                })
                .unwrap_or_default();
            Some(ContactAddress {
                kind: first_context(object.get("contexts")),
                formatted: object
                    .get("full")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                street,
                locality: object
                    .get("locality")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                region: object
                    .get("region")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                postal_code: object
                    .get("postcode")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                country: object
                    .get("country")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                is_primary: object.get("pref").and_then(Value::as_i64).unwrap_or(0) == 1,
            })
        })
        .collect()
}

fn notes(values: Option<&Map<String, Value>>) -> Option<String> {
    values.into_iter().flat_map(Map::values).find_map(|value| {
        value
            .as_object()?
            .get("note")?
            .as_str()
            .map(ToString::to_string)
    })
}

fn photo_url(values: Option<&Map<String, Value>>) -> Option<String> {
    values.into_iter().flat_map(Map::values).find_map(|value| {
        let object = value.as_object()?;
        (object.get("kind").and_then(Value::as_str) == Some("photo")).then_some(())?;
        object.get("uri")?.as_str().map(ToString::to_string)
    })
}

fn first_context(value: Option<&Value>) -> Option<String> {
    value?
        .as_object()?
        .iter()
        .find_map(|(key, value)| value.as_bool().unwrap_or(false).then(|| key.clone()))
}

fn phone_kind(object: &Map<String, Value>) -> Option<String> {
    first_feature(object.get("features")).or_else(|| first_context(object.get("contexts")))
}

fn first_feature(value: Option<&Value>) -> Option<String> {
    value?
        .as_object()?
        .iter()
        .find_map(|(key, value)| value.as_bool().unwrap_or(false).then(|| key.clone()))
}

fn decode_position(
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<i32, AccountError> {
    let Some(cursor) = page_cursor else {
        return Ok(0);
    };
    let cursor =
        String::from_utf8(cursor).map_err(|error| cursor_error(operation, error.to_string()))?;
    cursor
        .parse::<i32>()
        .map_err(|error| cursor_error(operation, error.to_string()))
}

fn next_cursor(position: i32, count: usize, total: Option<usize>) -> Option<Vec<u8>> {
    let count = i32::try_from(count).ok()?;
    let next = position.saturating_add(count);
    total
        .and_then(|total| i32::try_from(total).ok())
        .is_some_and(|total| next < total)
        .then(|| next.to_string().into_bytes())
}

fn usize_to_u64(value: usize, operation: AccountOperation) -> Result<u64, AccountError> {
    u64::try_from(value).map_err(|error| {
        super::error::unsupported_error(
            operation,
            None,
            format!("JMAP contact total does not fit u64: {error}"),
        )
    })
}

fn require_contacts(
    contacts: Option<ContactAccount>,
    operation: AccountOperation,
) -> Result<ContactAccount, AccountError> {
    contacts.ok_or_else(|| unsupported(operation, "JMAP contacts capability is unavailable"))
}

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(operation, None, message)
}

fn cursor_error(operation: AccountOperation, message: String) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::Jmap)
    .operation(operation)
    .text(DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn to_acct_err(operation: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(operation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jscontact_maps_to_shared_contact() {
        let card = JmapContactCard {
            properties: serde_json::from_value(json!({
                "id": "c1",
                "addressBookIds": {"ab1": true},
                "name": {"full": "Ada Lovelace"},
                "emails": {"e1": {"address": "ada@example.test", "contexts": {"work": true}, "pref": 1}},
                "phones": {"p1": {"number": "+123", "features": {"mobile": true}}},
                "addresses": {"a1": {"street": "1 Example St\nUnit 2", "locality": "London", "region": "England", "postcode": "N1", "country": "UK", "contexts": {"home": true}, "pref": 1}},
                "organizations": {"o1": {"name": "Analytical Engines", "title": "Programmer"}},
                "notes": {"n1": {"note": "notes"}},
                "media": {
                    "m1": {"kind": "logo", "uri": "https://example.test/logo.jpg"},
                    "m2": {"kind": "photo", "uri": "https://example.test/a.jpg"}
                }
            }))
            .expect("object"),
        };

        let contact = contact_from_jmap(card);
        assert_eq!(contact.id.0, "c1");
        // JMAP has no auto-collected corpus; every card routes to Main.
        assert_eq!(contact.corpus, ContactCorpus::Main);
        assert_eq!(contact.address_book_id.unwrap().0, "ab1");
        assert_eq!(contact.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.phones[0].kind.as_deref(), Some("mobile"));
        assert_eq!(contact.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(contact.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(contact.addresses[0].postal_code.as_deref(), Some("N1"));
        assert!(contact.addresses[0].is_primary);
        assert_eq!(
            contact.organizations[0].title.as_deref(),
            Some("Programmer")
        );
        assert_eq!(contact.notes.as_deref(), Some("notes"));
        assert_eq!(
            contact.photo_url.as_deref(),
            Some("https://example.test/a.jpg")
        );
    }

    #[test]
    fn contact_create_writes_card_type_and_photo_kind() {
        let create = jmap_create_from_contact(&ContactCreate {
            display_name: Some("Ada".to_string()),
            photo_url: Some("https://example.test/a.jpg".to_string()),
            ..ContactCreate::default()
        });

        assert_eq!(create.properties.get("@type"), Some(&json!("Card")));
        assert_eq!(create.properties["name"]["@type"], json!("Name"));
        let media = create.properties["media"]["photo"]
            .as_object()
            .expect("photo media object");
        assert_eq!(media.get("kind"), Some(&json!("photo")));
        assert_eq!(media.get("uri"), Some(&json!("https://example.test/a.jpg")));
    }

    #[test]
    fn contact_photo_round_trips_through_write_path() {
        // Exercise the write output through the read path rather than
        // hand-crafted JSON, so a wrong `kind` would surface as None.
        let create = jmap_create_from_contact(&ContactCreate {
            photo_url: Some("https://example.test/a.jpg".to_string()),
            ..ContactCreate::default()
        });
        let media = create
            .properties
            .get("media")
            .and_then(Value::as_object)
            .expect("media object");

        assert_eq!(
            photo_url(Some(media)).as_deref(),
            Some("https://example.test/a.jpg")
        );

        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                photo_url: Some(Some("https://example.test/b.jpg".to_string())),
                ..ContactPatch::default()
            },
            None,
        );
        let media = patch.properties["media"]
            .as_object()
            .expect("patch media object");
        assert_eq!(
            photo_url(Some(media)).as_deref(),
            Some("https://example.test/b.jpg")
        );
    }

    #[test]
    fn next_cursor_uses_total_and_position() {
        assert_eq!(next_cursor(0, 100, Some(250)), Some(Vec::from("100")));
        assert_eq!(next_cursor(200, 50, Some(250)), None);
        assert_eq!(next_cursor(0, 100, None), None);
    }

    #[test]
    fn decode_position_rejects_invalid_cursor() {
        let error = decode_position(
            Some(Vec::from("not-a-number")),
            AccountOperation::ContactsList,
        )
        .expect_err("invalid cursor should fail");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
            )
        ));
    }

    #[test]
    fn contact_create_maps_phone_type_to_features() {
        let create = jmap_create_from_contact(&ContactCreate {
            phones: vec![
                ContactPhone {
                    value: "+123".to_string(),
                    kind: Some("mobile".to_string()),
                    is_primary: true,
                },
                ContactPhone {
                    value: "+456".to_string(),
                    kind: Some("work".to_string()),
                    is_primary: false,
                },
            ],
            ..ContactCreate::default()
        });
        let phones = create
            .properties
            .get("phones")
            .and_then(Value::as_object)
            .expect("phones object");

        assert_eq!(phones["p0"]["features"]["mobile"].as_bool(), Some(true));
        assert!(phones["p0"]["contexts"].is_null());
        assert_eq!(phones["p1"]["contexts"]["work"].as_bool(), Some(true));
        assert!(phones["p1"]["features"].is_null());
    }

    #[test]
    fn contact_create_writes_addresses() {
        let create = jmap_create_from_contact(&ContactCreate {
            addresses: vec![ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: true,
            }],
            ..ContactCreate::default()
        });
        let addresses = create
            .properties
            .get("addresses")
            .and_then(Value::as_object)
            .expect("addresses object");

        assert_eq!(addresses["a0"]["contexts"]["work"].as_bool(), Some(true));
        assert_eq!(addresses["a0"]["street"].as_str(), Some("1 Analytical Way"));
        assert_eq!(addresses["a0"]["postcode"].as_str(), Some("N1"));
        assert_eq!(addresses["a0"]["pref"].as_i64(), Some(1));
    }

    #[test]
    fn contact_patch_clears_nullable_scalars_with_null() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                display_name: Some(None),
                notes: Some(None),
                photo_url: Some(None),
                ..ContactPatch::default()
            },
            None,
        );

        assert_eq!(patch.properties.get("name"), Some(&Value::Null));
        assert_eq!(patch.properties.get("notes"), Some(&Value::Null));
        assert_eq!(patch.properties.get("media"), Some(&Value::Null));
    }

    #[test]
    fn contact_patch_moves_address_book_by_nulling_old_book() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                address_book_id: Some(SharedAddressBookId("new".to_string())),
                ..ContactPatch::default()
            },
            Some(&SharedAddressBookId("old".to_string())),
        );
        let books = patch
            .properties
            .get("addressBookIds")
            .and_then(Value::as_object)
            .expect("addressBookIds");

        assert_eq!(books.get("old"), Some(&Value::Null));
        assert_eq!(books.get("new").and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn contact_patch_keeps_same_address_book_membership() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                address_book_id: Some(SharedAddressBookId("book".to_string())),
                ..ContactPatch::default()
            },
            Some(&SharedAddressBookId("book".to_string())),
        );
        let books = patch
            .properties
            .get("addressBookIds")
            .and_then(Value::as_object)
            .expect("addressBookIds");

        assert_eq!(books.len(), 1);
        assert_eq!(books.get("book").and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn absent_address_book_rights_are_not_writable() {
        assert!(!address_book_can_write(None));
        assert!(!address_book_can_delete(None));

        let rights = crate::address_book::AddressBookRights {
            may_read: Some(true),
            may_write: Some(true),
            may_share: None,
            may_delete: Some(false),
        };
        assert!(address_book_can_write(Some(&rights)));
        assert!(!address_book_can_delete(Some(&rights)));
    }
}
