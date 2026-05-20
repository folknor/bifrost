use serde::Deserialize;

pub const PEOPLE_API_BASE: &str = "https://people.googleapis.com/v1";
pub const PAGE_SIZE: u32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncContactsResult {
    pub synced: usize,
    pub deleted: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeopleConnectionsResponse {
    pub connections: Option<Vec<Person>>,
    pub next_page_token: Option<String>,
    pub next_sync_token: Option<String>,
    pub total_people: Option<i32>,
    pub total_items: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtherContactsResponse {
    pub other_contacts: Option<Vec<Person>>,
    pub next_page_token: Option<String>,
    pub next_sync_token: Option<String>,
    pub total_size: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Person {
    pub resource_name: Option<String>,
    pub etag: Option<String>,
    pub metadata: Option<PersonMetadata>,
    pub names: Option<Vec<Name>>,
    pub email_addresses: Option<Vec<EmailAddress>>,
    pub phone_numbers: Option<Vec<PhoneNumber>>,
    pub organizations: Option<Vec<Organization>>,
    pub photos: Option<Vec<Photo>>,
}

#[derive(Debug, Deserialize)]
pub struct PersonMetadata {
    pub deleted: Option<bool>,
    pub sources: Option<Vec<Source>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    #[serde(rename = "type")]
    pub source_type: Option<String>,
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Name {
    pub display_name: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailAddress {
    pub value: Option<String>,
    #[serde(rename = "type")]
    pub email_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhoneNumber {
    pub value: Option<String>,
    #[serde(rename = "type")]
    pub phone_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    pub name: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Photo {
    pub url: Option<String>,
}

pub fn people_api_base() -> &'static str {
    PEOPLE_API_BASE
}

pub fn extract_primary_email(person: &Person) -> Option<String> {
    person
        .email_addresses
        .as_ref()?
        .iter()
        .find_map(|e| e.value.as_deref().filter(|v| !v.is_empty()))
        .map(str::to_lowercase)
}

pub fn extract_display_name(person: &Person, fallback_email: &str) -> String {
    person
        .names
        .as_ref()
        .and_then(|names| names.first())
        .and_then(|n| n.display_name.as_deref())
        .filter(|n| !n.is_empty())
        .unwrap_or(fallback_email)
        .to_string()
}

pub fn extract_avatar_url(person: &Person) -> Option<String> {
    person.photos.as_ref()?.first().and_then(|p| p.url.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_person(email: Option<&str>, display_name: Option<&str>) -> Person {
        Person {
            resource_name: Some("people/1".to_string()),
            etag: None,
            metadata: None,
            names: display_name.map(|n| {
                vec![Name {
                    display_name: Some(n.to_string()),
                    given_name: None,
                    family_name: None,
                }]
            }),
            email_addresses: email.map(|e| {
                vec![EmailAddress {
                    value: Some(e.to_string()),
                    email_type: Some("home".to_string()),
                }]
            }),
            phone_numbers: None,
            organizations: None,
            photos: None,
        }
    }

    #[test]
    fn deserializes_people_response() {
        let json = r#"{
            "connections": [
                {
                    "resourceName": "people/c12345",
                    "etag": "abc",
                    "names": [{"displayName": "Alice Smith"}],
                    "emailAddresses": [{"value": "alice@example.com", "type": "home"}]
                }
            ],
            "nextSyncToken": "sync_token_abc",
            "totalPeople": 1,
            "totalItems": 1
        }"#;

        let response: PeopleConnectionsResponse = serde_json::from_str(json).expect("deserialize");
        let connections = response.connections.as_ref().expect("connections");
        assert_eq!(connections.len(), 1);
        assert_eq!(
            connections[0].resource_name.as_deref(),
            Some("people/c12345")
        );
        assert_eq!(response.next_sync_token.as_deref(), Some("sync_token_abc"));
    }

    #[test]
    fn extracts_primary_email() {
        let person = make_person(Some("Alice@Example.COM"), None);
        assert_eq!(
            extract_primary_email(&person),
            Some("alice@example.com".to_string())
        );
    }

    #[test]
    fn extracts_display_name_with_fallback() {
        let named = make_person(Some("alice@example.com"), Some("Alice Smith"));
        assert_eq!(
            extract_display_name(&named, "alice@example.com"),
            "Alice Smith"
        );

        let unnamed = make_person(Some("alice@example.com"), None);
        assert_eq!(
            extract_display_name(&unnamed, "alice@example.com"),
            "alice@example.com"
        );
    }
}
