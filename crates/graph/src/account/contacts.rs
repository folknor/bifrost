use bifrost_types::{
    AccountError, AccountOperation, AddressBook, AddressBookId, ContactAddress, ContactCard,
    ContactCreate, ContactEmail, ContactId, ContactOrganization, ContactPatch, ContactPhone,
    ContactProvenance, ContactSearchRequest, ErrorScope, Page, ProtocolKind,
};
use serde::{Deserialize, Serialize};

use crate::types::ODataCollection;

use super::GraphAccount;
use super::graph_error::{self, GraphErrorContext};

const DEFAULT_CONTACTS_ID: &str = "contacts";
const CONTACT_SELECT: &str = "id,displayName,emailAddresses,businessPhones,homePhones,mobilePhone,homeAddress,businessAddress,otherAddress,companyName,jobTitle,personalNotes,parentFolderId";

pub(crate) async fn address_books_list(
    account: GraphAccount,
) -> Result<Vec<AddressBook>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let mut books = vec![AddressBook {
        id: AddressBookId(DEFAULT_CONTACTS_ID.to_string()),
        native_id: DEFAULT_CONTACTS_ID.to_string(),
        name: "Contacts".to_string(),
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: DEFAULT_CONTACTS_ID.to_string(),
            address_book_native: None,
        },
        is_default: true,
        can_create_contacts: true,
        can_update_contacts: true,
        can_delete_contacts: true,
    }];
    let mut next = Some(format!(
        "{prefix}/contactFolders?$select=id,displayName,parentFolderId&$top=250"
    ));
    while let Some(url) = next {
        let page: ODataCollection<GraphContactFolder> =
            get_page(&account, &url, AccountOperation::AddressBooksList).await?;
        books.extend(page.value.into_iter().map(folder_to_address_book));
        next = page.next_link;
    }
    Ok(books)
}

pub(crate) async fn list(
    account: GraphAccount,
    address_book: Option<AddressBookId>,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<ContactCard>, AccountError> {
    contact_page(account, address_book, page_cursor, 250).await
}

async fn contact_page(
    account: GraphAccount,
    address_book: Option<AddressBookId>,
    page_cursor: Option<Vec<u8>>,
    top: u32,
) -> Result<Page<ContactCard>, AccountError> {
    let next_url = page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| {
            graph_error::unsupported_account_error(AccountOperation::ContactsList)
                .into_builder()
                .scope(ErrorScope::ContactCollection)
                .text(bifrost_types::DiagnosticText::support_only(
                    error.to_string(),
                ))
                .try_build()
                .expect("valid account error classification")
        })?;
    let url = match next_url {
        Some(url) => url,
        None => contacts_url(&account, address_book.as_ref(), top),
    };
    let page: ODataCollection<GraphContact> =
        get_page(&account, &url, AccountOperation::ContactsList).await?;
    Ok(Page {
        items: page.value.into_iter().map(contact_from_graph).collect(),
        next_cursor: page.next_link.map(String::into_bytes),
        estimated_total: None,
    })
}

pub(crate) async fn get(
    account: GraphAccount,
    contact: ContactId,
) -> Result<ContactCard, AccountError> {
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}?$select={CONTACT_SELECT}");
    let contact = account
        .client
        .get_json::<GraphContact>(&path)
        .await
        .map_err(|error| into_error(error, AccountOperation::ContactGet))?;
    Ok(contact_from_graph(contact))
}

pub(crate) async fn create(
    account: GraphAccount,
    contact: ContactCreate,
) -> Result<ContactId, AccountError> {
    let path = create_url(&account, contact.address_book_id.as_ref());
    let created = account
        .client
        .post::<GraphContact, _>(&path, &graph_contact_from_create(&contact))
        .await
        .map_err(|error| into_error(error, AccountOperation::ContactCreate))?;
    Ok(ContactId(created.id))
}

pub(crate) async fn update(
    account: GraphAccount,
    contact: ContactId,
    patch: ContactPatch,
) -> Result<(), AccountError> {
    if patch.photo.is_some() {
        return Err(
            graph_error::unsupported_account_error(AccountOperation::ContactUpdate)
                .into_builder()
                .text(bifrost_types::DiagnosticText::support_only(
                    "Graph contact raw photo updates are unsupported".to_string(),
                ))
                .try_build()
                .expect("valid account error classification"),
        );
    }
    let current = get(account.clone(), contact.clone()).await?;
    let etag = current.etag.clone();
    let merged = merge_patch(current, &patch);
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}");
    let body = graph_contact_from_create(&merged);
    let result = if let Some(etag) = etag.as_deref() {
        account.client.patch_if_match(&path, etag, &body).await
    } else {
        account.client.patch(&path, &body).await
    };
    result.map_err(|error| into_error(error, AccountOperation::ContactUpdate))
}

pub(crate) async fn delete(account: GraphAccount, contact: ContactId) -> Result<(), AccountError> {
    let current = get(account.clone(), contact.clone()).await?;
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}");
    let result = if let Some(etag) = current.etag.as_deref() {
        account.client.delete_if_match(&path, etag).await
    } else {
        account.client.delete(&path).await
    };
    result.map_err(|error| into_error(error, AccountOperation::ContactDelete))
}

pub(crate) async fn search(
    account: GraphAccount,
    request: ContactSearchRequest,
) -> Result<Page<ContactCard>, AccountError> {
    let limit = request
        .limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(250)
        .max(1);
    let next_url = request
        .page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| {
            graph_error::unsupported_account_error(AccountOperation::ContactSearch)
                .into_builder()
                .scope(ErrorScope::ContactCollection)
                .text(bifrost_types::DiagnosticText::support_only(
                    error.to_string(),
                ))
                .try_build()
                .expect("valid account error classification")
        })?;
    let mut url = next_url.unwrap_or_else(|| {
        contact_search_url(
            &account,
            request.address_book_id.as_ref(),
            &request.query,
            request.limit.unwrap_or(250).min(250),
        )
    });
    let needle = request.query.to_ascii_lowercase();
    let mut items = Vec::new();
    let next_cursor;
    loop {
        let page: ODataCollection<GraphContact> =
            get_page(&account, &url, AccountOperation::ContactSearch).await?;
        items.extend(
            page.value
                .into_iter()
                .map(contact_from_graph)
                .filter(|contact| contact_matches(contact, &needle)),
        );
        if items.len() >= limit {
            items.truncate(limit);
            next_cursor = page.next_link.map(String::into_bytes);
            break;
        }
        let Some(next) = page.next_link else {
            next_cursor = None;
            break;
        };
        url = next;
    }
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
    })
}

async fn get_page<T: serde::de::DeserializeOwned>(
    account: &GraphAccount,
    url: &str,
    operation: AccountOperation,
) -> Result<ODataCollection<T>, AccountError> {
    if url.starts_with("http") {
        account
            .client
            .get_absolute(url)
            .await
            .map_err(|error| into_error(error, operation))
    } else {
        account
            .client
            .get_json(url)
            .await
            .map_err(|error| into_error(error, operation))
    }
}

fn contacts_url(account: &GraphAccount, address_book: Option<&AddressBookId>, top: u32) -> String {
    let prefix = account.client.api_path_prefix();
    contacts_path(&prefix, address_book, top)
}

fn contact_search_url(
    account: &GraphAccount,
    address_book: Option<&AddressBookId>,
    query: &str,
    top: u32,
) -> String {
    let prefix = account.client.api_path_prefix();
    contact_search_path(&prefix, address_book, query, top)
}

fn contacts_path(prefix: &str, address_book: Option<&AddressBookId>, top: u32) -> String {
    match address_book.map(|id| id.0.as_str()) {
        Some(DEFAULT_CONTACTS_ID) | None => {
            format!("{prefix}/contacts?$select={CONTACT_SELECT}&$top={top}")
        }
        Some(folder) => {
            let encoded = bifrost_net::url::encode_component(folder);
            format!(
                "{prefix}/contactFolders/{encoded}/contacts?$select={CONTACT_SELECT}&$top={top}"
            )
        }
    }
}

fn contact_search_path(
    prefix: &str,
    address_book: Option<&AddressBookId>,
    query: &str,
    top: u32,
) -> String {
    let base = contacts_path(prefix, address_book, top);
    let trimmed = query.trim();
    if !is_exact_email_query(trimmed) {
        return base;
    }
    let escaped = trimmed.replace('\'', "''");
    let filter = format!("emailAddresses/any(a:a/address eq '{escaped}')");
    format!(
        "{base}&$filter={}",
        bifrost_net::url::encode_component(&filter)
    )
}

fn is_exact_email_query(query: &str) -> bool {
    !query.is_empty() && query.contains('@') && !query.chars().any(char::is_whitespace)
}

fn create_url(account: &GraphAccount, address_book: Option<&AddressBookId>) -> String {
    let prefix = account.client.api_path_prefix();
    match address_book.map(|id| id.0.as_str()) {
        Some(DEFAULT_CONTACTS_ID) | None => format!("{prefix}/contacts"),
        Some(folder) => {
            let encoded = bifrost_net::url::encode_component(folder);
            format!("{prefix}/contactFolders/{encoded}/contacts")
        }
    }
}

fn folder_to_address_book(folder: GraphContactFolder) -> AddressBook {
    AddressBook {
        id: AddressBookId(folder.id.clone()),
        native_id: folder.id.clone(),
        name: folder.display_name.unwrap_or(folder.id.clone()),
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: folder.id,
            address_book_native: None,
        },
        is_default: false,
        can_create_contacts: true,
        can_update_contacts: true,
        can_delete_contacts: true,
    }
}

fn contact_from_graph(contact: GraphContact) -> ContactCard {
    let addresses = graph_addresses(&contact);
    let mut phones = contact
        .business_phones
        .unwrap_or_default()
        .into_iter()
        .filter(|value| !value.is_empty())
        .map(|value| ContactPhone {
            value,
            kind: Some("business".to_string()),
            is_primary: false,
        })
        .collect::<Vec<_>>();
    phones.extend(
        contact
            .home_phones
            .unwrap_or_default()
            .into_iter()
            .filter(|value| !value.is_empty())
            .map(|value| ContactPhone {
                value,
                kind: Some("home".to_string()),
                is_primary: false,
            }),
    );
    if let Some(mobile) = contact.mobile_phone.filter(|value| !value.is_empty()) {
        phones.push(ContactPhone {
            value: mobile,
            kind: Some("mobile".to_string()),
            is_primary: phones.is_empty(),
        });
    }
    ContactCard {
        id: ContactId(contact.id.clone()),
        address_book_id: contact.parent_folder_id.clone().map(AddressBookId),
        native_id: contact.id.clone(),
        etag: contact.etag,
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: contact.id,
            address_book_native: contact.parent_folder_id,
        },
        display_name: contact.display_name,
        emails: contact
            .email_addresses
            .unwrap_or_default()
            .into_iter()
            .filter_map(|email| {
                Some(ContactEmail {
                    value: email.address?,
                    kind: email.name,
                    is_primary: false,
                })
            })
            .collect(),
        phones,
        organizations: contact
            .company_name
            .map(|name| {
                vec![ContactOrganization {
                    name,
                    title: contact.job_title,
                }]
            })
            .unwrap_or_default(),
        addresses,
        notes: contact.personal_notes,
        photo_url: None,
        photo: None,
    }
}

fn graph_contact_from_create(contact: &ContactCreate) -> GraphContactPatch {
    GraphContactPatch {
        display_name: contact.display_name.clone(),
        email_addresses: non_empty(contact.emails.iter().map(|email| GraphContactEmail {
            name: email.kind.clone(),
            address: Some(email.value.clone()),
        })),
        business_phones: non_empty(
            contact
                .phones
                .iter()
                .filter(|phone| !matches!(phone.kind.as_deref(), Some("mobile") | Some("home")))
                .map(|phone| phone.value.clone()),
        ),
        home_phones: non_empty(
            contact
                .phones
                .iter()
                .filter(|phone| phone.kind.as_deref() == Some("home"))
                .map(|phone| phone.value.clone()),
        ),
        mobile_phone: contact
            .phones
            .iter()
            .find(|phone| phone.kind.as_deref() == Some("mobile"))
            .map(|phone| phone.value.clone()),
        home_address: graph_address_from_shared(contact, "home"),
        business_address: graph_address_from_shared(contact, "work"),
        other_address: contact
            .addresses
            .iter()
            .find(|address| !matches!(address.kind.as_deref(), Some("home" | "work")))
            .map(graph_address),
        company_name: contact.organizations.first().map(|org| org.name.clone()),
        job_title: contact
            .organizations
            .first()
            .and_then(|org| org.title.clone()),
        personal_notes: contact.notes.clone(),
    }
}

fn graph_addresses(contact: &GraphContact) -> Vec<ContactAddress> {
    [
        ("home", contact.home_address.as_ref()),
        ("work", contact.business_address.as_ref()),
        ("other", contact.other_address.as_ref()),
    ]
    .into_iter()
    .filter_map(|(kind, address)| {
        let address = address?;
        Some(ContactAddress {
            kind: Some(kind.to_string()),
            formatted: None,
            street: address
                .street
                .as_deref()
                .map(|street| street.lines().map(ToString::to_string).collect())
                .unwrap_or_default(),
            locality: address.city.clone(),
            region: address.state.clone(),
            postal_code: address.postal_code.clone(),
            country: address.country_or_region.clone(),
            is_primary: false,
        })
    })
    .collect()
}

fn graph_address_from_shared(contact: &ContactCreate, kind: &str) -> Option<GraphPhysicalAddress> {
    contact
        .addresses
        .iter()
        .find(|address| address.kind.as_deref() == Some(kind))
        .map(graph_address)
}

fn graph_address(address: &ContactAddress) -> GraphPhysicalAddress {
    GraphPhysicalAddress {
        street: (!address.street.is_empty()).then(|| address.street.join("\n")),
        city: address.locality.clone(),
        state: address.region.clone(),
        postal_code: address.postal_code.clone(),
        country_or_region: address.country.clone(),
    }
}

fn merge_patch(current: ContactCard, patch: &ContactPatch) -> ContactCreate {
    ContactCreate {
        address_book_id: patch.address_book_id.clone().or(current.address_book_id),
        display_name: patch.display_name.clone().unwrap_or(current.display_name),
        emails: patch.emails.clone().unwrap_or(current.emails),
        phones: patch.phones.clone().unwrap_or(current.phones),
        organizations: patch.organizations.clone().unwrap_or(current.organizations),
        addresses: patch.addresses.clone().unwrap_or(current.addresses),
        notes: patch.notes.clone().unwrap_or(current.notes),
        photo_url: patch.photo_url.clone().unwrap_or(current.photo_url),
    }
}

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn contact_matches(contact: &ContactCard, needle: &str) -> bool {
    needle.is_empty()
        || contact
            .display_name
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || contact
            .emails
            .iter()
            .any(|email| contains(&email.value, needle))
        || contact
            .phones
            .iter()
            .any(|phone| contains(&phone.value, needle))
        || contact.organizations.iter().any(|org| {
            contains(&org.name, needle)
                || org
                    .title
                    .as_deref()
                    .is_some_and(|title| contains(title, needle))
        })
        || contact.addresses.iter().any(|address| {
            address
                .formatted
                .as_deref()
                .is_some_and(|value| contains(value, needle))
                || address.street.iter().any(|value| contains(value, needle))
                || address
                    .locality
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .region
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .postal_code
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .country
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
        })
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_ascii_lowercase().contains(needle)
}

fn into_error(error: crate::error::GraphError, operation: AccountOperation) -> AccountError {
    graph_error::into_account_error(error, GraphErrorContext::graph(operation))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactFolder {
    id: String,
    display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphContact {
    id: String,
    #[serde(rename = "@odata.etag")]
    etag: Option<String>,
    display_name: Option<String>,
    email_addresses: Option<Vec<GraphContactEmail>>,
    business_phones: Option<Vec<String>>,
    home_phones: Option<Vec<String>>,
    mobile_phone: Option<String>,
    home_address: Option<GraphPhysicalAddress>,
    business_address: Option<GraphPhysicalAddress>,
    other_address: Option<GraphPhysicalAddress>,
    company_name: Option<String>,
    job_title: Option<String>,
    personal_notes: Option<String>,
    parent_folder_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactEmail {
    name: Option<String>,
    address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphPhysicalAddress {
    street: Option<String>,
    city: Option<String>,
    state: Option<String>,
    postal_code: Option<String>,
    country_or_region: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email_addresses: Option<Vec<GraphContactEmail>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_phones: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_phones: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mobile_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    other_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    company_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    personal_notes: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_contact_maps_to_contact_card() {
        let card = contact_from_graph(GraphContact {
            id: "contact-1".to_string(),
            etag: Some("etag-1".to_string()),
            display_name: Some("Ada Lovelace".to_string()),
            email_addresses: Some(vec![GraphContactEmail {
                name: Some("work".to_string()),
                address: Some("ada@example.test".to_string()),
            }]),
            business_phones: Some(vec!["+123".to_string()]),
            home_phones: Some(vec!["+124".to_string()]),
            mobile_phone: Some("+456".to_string()),
            home_address: Some(GraphPhysicalAddress {
                street: Some("1 Example St\nUnit 2".to_string()),
                city: Some("London".to_string()),
                state: Some("England".to_string()),
                postal_code: Some("N1".to_string()),
                country_or_region: Some("UK".to_string()),
            }),
            business_address: None,
            other_address: None,
            company_name: Some("Analytical Engines".to_string()),
            job_title: Some("Programmer".to_string()),
            personal_notes: Some("notes".to_string()),
            parent_folder_id: Some("folder-1".to_string()),
        });

        assert_eq!(card.id.0, "contact-1");
        assert_eq!(card.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(card.etag.as_deref(), Some("etag-1"));
        assert_eq!(card.emails[0].kind.as_deref(), Some("work"));
        assert_eq!(card.phones[0].kind.as_deref(), Some("business"));
        assert_eq!(card.phones[1].kind.as_deref(), Some("home"));
        assert_eq!(card.phones[2].kind.as_deref(), Some("mobile"));
        assert_eq!(card.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(card.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(card.addresses[0].locality.as_deref(), Some("London"));
        assert_eq!(card.organizations[0].title.as_deref(), Some("Programmer"));
        assert_eq!(card.notes.as_deref(), Some("notes"));
    }

    #[test]
    fn create_payload_maps_contact_fields() {
        let payload = graph_contact_from_create(&ContactCreate {
            display_name: Some("Ada".to_string()),
            emails: vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("work".to_string()),
                is_primary: true,
            }],
            phones: vec![
                ContactPhone {
                    value: "+456".to_string(),
                    kind: Some("mobile".to_string()),
                    is_primary: true,
                },
                ContactPhone {
                    value: "+123".to_string(),
                    kind: Some("home".to_string()),
                    is_primary: false,
                },
            ],
            notes: Some("notes".to_string()),
            addresses: vec![ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: false,
            }],
            ..ContactCreate::default()
        });

        assert_eq!(payload.display_name.as_deref(), Some("Ada"));
        assert_eq!(
            payload.email_addresses.unwrap()[0].address.as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(payload.mobile_phone.as_deref(), Some("+456"));
        assert_eq!(
            payload
                .business_address
                .as_ref()
                .and_then(|address| address.street.as_deref()),
            Some("1 Analytical Way")
        );
        assert_eq!(payload.home_phones.unwrap()[0], "+123");
        assert_eq!(payload.personal_notes.as_deref(), Some("notes"));
    }

    #[test]
    fn contacts_path_uses_requested_page_size() {
        assert_eq!(
            contacts_path("/me", None, 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
        assert_eq!(
            contacts_path(
                "/me",
                Some(&AddressBookId("folder with spaces".to_string())),
                10
            ),
            format!(
                "/me/contactFolders/folder%20with%20spaces/contacts?$select={CONTACT_SELECT}&$top=10"
            )
        );
    }

    #[test]
    fn contact_search_path_filters_exact_email_queries() {
        assert_eq!(
            contact_search_path("/me", None, "ada@example.test", 25),
            format!(
                "/me/contacts?$select={CONTACT_SELECT}&$top=25&$filter=emailAddresses%2Fany%28a%3Aa%2Faddress%20eq%20%27ada%40example.test%27%29"
            )
        );
        assert_eq!(
            contact_search_path(
                "/me",
                Some(&AddressBookId("folder".to_string())),
                "ada@example.test",
                10
            ),
            format!(
                "/me/contactFolders/folder/contacts?$select={CONTACT_SELECT}&$top=10&$filter=emailAddresses%2Fany%28a%3Aa%2Faddress%20eq%20%27ada%40example.test%27%29"
            )
        );
    }

    #[test]
    fn contact_search_path_keeps_local_scan_for_substring_queries() {
        assert_eq!(
            contact_search_path("/me", None, "ada", 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
        assert_eq!(
            contact_search_path("/me", None, "ada lovelace@example.test", 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
    }
}
