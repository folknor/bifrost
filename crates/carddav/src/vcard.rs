use base64::Engine;
use bifrost_types::{
    AddressBookId, ContactAddress, ContactCard, ContactCreate, ContactEmail, ContactId,
    ContactOrganization, ContactPatch, ContactPhone, ContactPhoto, ContactProvenance, ProtocolKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedVCard {
    pub(crate) display_name: Option<String>,
    pub(crate) emails: Vec<ContactEmail>,
    pub(crate) phones: Vec<ContactPhone>,
    pub(crate) organizations: Vec<ContactOrganization>,
    pub(crate) addresses: Vec<ContactAddress>,
    pub(crate) notes: Option<String>,
    pub(crate) photo_url: Option<String>,
    pub(crate) photo: Option<ContactPhoto>,
}

pub(crate) fn contact_from_vcard(
    uri: String,
    address_book_id: Option<AddressBookId>,
    etag: Option<String>,
    data: &str,
) -> ContactCard {
    let parsed = parse_vcard(data);
    ContactCard {
        id: ContactId(uri.clone()),
        address_book_id: address_book_id.clone(),
        native_id: uri.clone(),
        etag,
        provenance: ContactProvenance {
            provider: ProtocolKind::CardDav,
            native: uri,
            address_book_native: address_book_id.map(|id| id.0),
        },
        display_name: parsed.display_name,
        emails: parsed.emails,
        phones: parsed.phones,
        organizations: parsed.organizations,
        addresses: parsed.addresses,
        notes: parsed.notes,
        photo_url: parsed.photo_url,
        photo: parsed.photo,
    }
}

pub(crate) fn vcard_from_create(contact: &ContactCreate, uid: &str) -> String {
    let mut lines = vec![
        "BEGIN:VCARD".to_string(),
        "VERSION:4.0".to_string(),
        format!("UID:{}", escape_text(uid)),
    ];
    append_contact_fields(&mut lines, contact);
    lines.push("END:VCARD".to_string());
    fold_vcard_lines(lines)
}

pub(crate) fn vcard_from_patch(
    current: &ContactCard,
    source: &str,
    patch: &ContactPatch,
) -> String {
    let mut preserved = Vec::new();
    let mut insert_at = None;
    let mut saw_fn = false;
    for line in unfold_lines(source) {
        let property = property_name(&line);
        if property.as_deref() == Some("END") {
            insert_at = Some(preserved.len());
        }
        if property.as_deref() == Some("FN") {
            saw_fn = true;
        }
        if should_replace_property(property.as_deref(), patch) {
            continue;
        }
        preserved.push(line);
    }

    let mut replacement = Vec::new();
    if let Some(display_name) = &patch.display_name {
        append_fn(&mut replacement, display_name.as_deref());
    } else if !saw_fn {
        append_fn(&mut replacement, current.display_name.as_deref());
    }
    if let Some(emails) = &patch.emails {
        append_emails(&mut replacement, emails);
    }
    if let Some(phones) = &patch.phones {
        append_phones(&mut replacement, phones);
    }
    if let Some(organizations) = &patch.organizations {
        append_organizations(&mut replacement, organizations);
    }
    if let Some(addresses) = &patch.addresses {
        append_addresses(&mut replacement, addresses);
    }
    if let Some(notes) = &patch.notes {
        append_notes(&mut replacement, notes.as_deref());
    }
    if let Some(photo_url) = &patch.photo_url {
        append_photo(&mut replacement, photo_url.as_deref());
    }
    if let Some(photo) = &patch.photo {
        append_inline_photo(&mut replacement, photo.as_ref());
    }

    let at = insert_at.unwrap_or(preserved.len());
    preserved.splice(at..at, replacement);
    fold_vcard_lines(preserved)
}

fn append_contact_fields(lines: &mut Vec<String>, contact: &ContactCreate) {
    append_fn(lines, contact.display_name.as_deref());
    append_emails(lines, &contact.emails);
    append_phones(lines, &contact.phones);
    append_organizations(lines, &contact.organizations);
    append_addresses(lines, &contact.addresses);
    append_notes(lines, contact.notes.as_deref());
    append_photo(lines, contact.photo_url.as_deref());
}

fn append_fn(lines: &mut Vec<String>, display_name: Option<&str>) {
    lines.push(format!("FN:{}", escape_text(display_name.unwrap_or(""))));
}

fn append_emails(lines: &mut Vec<String>, emails: &[ContactEmail]) {
    for email in emails {
        let params = type_param(email.kind.as_deref(), email.is_primary);
        lines.push(format!("EMAIL{params}:{}", escape_text(&email.value)));
    }
}

fn append_phones(lines: &mut Vec<String>, phones: &[ContactPhone]) {
    for phone in phones {
        let params = type_param(phone.kind.as_deref(), phone.is_primary);
        lines.push(format!("TEL{params}:{}", escape_text(&phone.value)));
    }
}

fn append_organizations(lines: &mut Vec<String>, organizations: &[ContactOrganization]) {
    for organization in organizations {
        lines.push(format!("ORG:{}", escape_text(&organization.name)));
        if let Some(title) = organization.title.as_deref() {
            lines.push(format!("TITLE:{}", escape_text(title)));
        }
    }
}

fn append_addresses(lines: &mut Vec<String>, addresses: &[ContactAddress]) {
    for address in addresses {
        let params = type_param(address.kind.as_deref(), address.is_primary);
        let street = address
            .street
            .iter()
            .map(|value| escape_text(value))
            .collect::<Vec<_>>()
            .join("\\n");
        lines.push(format!(
            "ADR{params}:;;{};{};{};{};{}",
            street,
            escape_text(address.locality.as_deref().unwrap_or_default()),
            escape_text(address.region.as_deref().unwrap_or_default()),
            escape_text(address.postal_code.as_deref().unwrap_or_default()),
            escape_text(address.country.as_deref().unwrap_or_default())
        ));
    }
}

fn append_notes(lines: &mut Vec<String>, notes: Option<&str>) {
    if let Some(notes) = notes {
        lines.push(format!("NOTE:{}", escape_text(notes)));
    }
}

fn append_photo(lines: &mut Vec<String>, photo_url: Option<&str>) {
    if let Some(photo_url) = photo_url {
        lines.push(format!("PHOTO;VALUE=URI:{}", escape_text(photo_url)));
    }
}

fn append_inline_photo(lines: &mut Vec<String>, photo: Option<&ContactPhoto>) {
    if let Some(photo) = photo {
        let mut line = String::from("PHOTO;ENCODING=b");
        if let Some(media_type) = photo
            .media_type
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            line.push_str(";TYPE=");
            line.push_str(&escape_param(media_type));
        }
        line.push(':');
        line.push_str(&base64::engine::general_purpose::STANDARD.encode(&photo.data));
        lines.push(line);
    }
}

fn should_replace_property(property: Option<&str>, patch: &ContactPatch) -> bool {
    match property {
        Some("FN") => patch.display_name.is_some(),
        Some("EMAIL") => patch.emails.is_some(),
        Some("TEL") => patch.phones.is_some(),
        Some("ORG" | "TITLE") => patch.organizations.is_some(),
        Some("ADR") => patch.addresses.is_some(),
        Some("NOTE") => patch.notes.is_some(),
        Some("PHOTO") => patch.photo_url.is_some() || patch.photo.is_some(),
        _ => false,
    }
}

fn property_name(line: &str) -> Option<String> {
    let (name_and_params, _) = line.split_once(':').unwrap_or((line, ""));
    let name = name_and_params
        .split_once(';')
        .map(|(name, _)| name)
        .unwrap_or(name_and_params);
    let name = name.rsplit_once('.').map(|(_, name)| name).unwrap_or(name);
    if name.is_empty() {
        None
    } else {
        Some(name.to_ascii_uppercase())
    }
}

fn parse_vcard(data: &str) -> ParsedVCard {
    let mut parsed = ParsedVCard {
        display_name: None,
        emails: Vec::new(),
        phones: Vec::new(),
        organizations: Vec::new(),
        addresses: Vec::new(),
        notes: None,
        photo_url: None,
        photo: None,
    };
    let mut pending_org: Option<ContactOrganization> = None;
    let mut structured_name: Option<String> = None;

    for line in unfold_lines(data) {
        let Some((name_and_params, raw_value)) = line.split_once(':') else {
            continue;
        };
        let mut parts = name_and_params.split(';');
        let name = parts.next().unwrap_or_default().to_ascii_uppercase();
        let params = parts.collect::<Vec<_>>();
        let value = unescape_text(raw_value.trim());
        match name.as_str() {
            "FN" if !value.is_empty() => parsed.display_name = Some(value),
            "N" if !value.is_empty() => {
                structured_name = Some(display_name_from_n(raw_value.trim()));
            }
            "EMAIL" if !value.is_empty() => parsed.emails.push(ContactEmail {
                value,
                kind: type_from_params(&params),
                is_primary: is_primary(&params),
            }),
            "TEL" if !value.is_empty() => parsed.phones.push(ContactPhone {
                value,
                kind: type_from_params(&params),
                is_primary: is_primary(&params),
            }),
            "ORG" if !value.is_empty() => {
                if let Some(org) = pending_org.take() {
                    parsed.organizations.push(org);
                }
                pending_org = Some(ContactOrganization {
                    name: split_components(raw_value.trim())
                        .into_iter()
                        .filter(|part| !part.is_empty())
                        .collect::<Vec<_>>()
                        .join(" "),
                    title: None,
                });
            }
            "TITLE" if !value.is_empty() => {
                if let Some(org) = pending_org.as_mut() {
                    org.title = Some(value);
                }
            }
            "ADR" if !raw_value.trim().is_empty() => {
                parsed
                    .addresses
                    .push(address_from_adr(raw_value.trim(), &params));
            }
            "NOTE" if !value.is_empty() => parsed.notes = Some(value),
            "PHOTO" if is_url(&value) => parsed.photo_url = Some(value),
            "PHOTO" => parsed.photo = inline_photo(raw_value.trim(), &params),
            _ => {}
        }
    }

    if let Some(org) = pending_org {
        parsed.organizations.push(org);
    }
    if parsed.display_name.is_none() {
        parsed.display_name = structured_name.filter(|name| !name.is_empty());
    }
    parsed
}

fn address_from_adr(value: &str, params: &[&str]) -> ContactAddress {
    let parts = split_components(value);
    let street = [parts.first(), parts.get(1), parts.get(2)]
        .into_iter()
        .flatten()
        .flat_map(|part| part.split('\n'))
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect();
    ContactAddress {
        kind: type_from_params(params),
        formatted: None,
        street,
        locality: non_empty_part(parts.get(3)),
        region: non_empty_part(parts.get(4)),
        postal_code: non_empty_part(parts.get(5)),
        country: non_empty_part(parts.get(6)),
        is_primary: is_primary(params),
    }
}

fn non_empty_part(value: Option<&String>) -> Option<String> {
    value.filter(|value| !value.is_empty()).cloned()
}

fn display_name_from_n(value: &str) -> String {
    let parts = split_components(value);
    let family = parts.first().map(String::as_str).unwrap_or_default();
    let given = parts.get(1).map(String::as_str).unwrap_or_default();
    let additional = parts.get(2).map(String::as_str).unwrap_or_default();
    let prefix = parts.get(3).map(String::as_str).unwrap_or_default();
    let suffix = parts.get(4).map(String::as_str).unwrap_or_default();
    [prefix, given, additional, family, suffix]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_components(value: &str) -> Vec<String> {
    let mut components = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            current.push('\\');
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ';' {
            components.push(unescape_text(&current));
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if escaped {
        current.push('\\');
    }
    components.push(unescape_text(&current));
    components
}

fn unfold_lines(data: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for raw in data.lines() {
        let line = raw.trim_end_matches('\r');
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(previous) = lines.last_mut() {
                previous.push_str(line.trim_start());
            }
        } else {
            lines.push(line.to_string());
        }
    }
    lines
}

fn type_param(kind: Option<&str>, primary: bool) -> String {
    let mut params = Vec::new();
    if let Some(kind) = kind.filter(|kind| !kind.is_empty()) {
        params.push(format!("TYPE={}", escape_param(kind)));
    }
    if primary {
        params.push("PREF=1".to_string());
    }
    if params.is_empty() {
        String::new()
    } else {
        format!(";{}", params.join(";"))
    }
}

fn type_from_params(params: &[&str]) -> Option<String> {
    params.iter().find_map(|param| {
        if let Some((key, value)) = param.split_once('=') {
            if key.eq_ignore_ascii_case("TYPE") {
                Some(value.trim_matches('"').to_ascii_lowercase())
            } else {
                None
            }
        } else if !param.is_empty() {
            Some(param.trim_matches('"').to_ascii_lowercase())
        } else {
            None
        }
    })
}

fn is_primary(params: &[&str]) -> bool {
    params.iter().any(|param| {
        if param.eq_ignore_ascii_case("PREF") {
            return true;
        }
        let Some((key, value)) = param.split_once('=') else {
            return false;
        };
        key.eq_ignore_ascii_case("PREF") && value == "1"
    })
}

fn escape_param(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', "")
        .replace('\n', "\\n");
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

fn fold_vcard_lines(lines: Vec<String>) -> String {
    let mut folded = String::new();
    for line in lines {
        let mut current = String::new();
        for ch in line.chars() {
            if current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
            }
            current.push(ch);
        }
        folded.push_str(&current);
        folded.push_str("\r\n");
    }
    folded
}

fn escape_text(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(';', "\\;")
        .replace(',', "\\,")
}

fn unescape_text(value: &str) -> String {
    let mut output = String::new();
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                'n' | 'N' => output.push('\n'),
                '\\' | ';' | ',' => output.push(ch),
                _ => {
                    output.push('\\');
                    output.push(ch);
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        output.push('\\');
    }
    output
}

fn is_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn inline_photo(value: &str, params: &[&str]) -> Option<ContactPhoto> {
    let has_inline_encoding = params.iter().any(|param| {
        param.eq_ignore_ascii_case("ENCODING=b")
            || param.eq_ignore_ascii_case("ENCODING=BASE64")
            || param.eq_ignore_ascii_case("VALUE=BINARY")
    });
    if !has_inline_encoding {
        return None;
    }
    let compact = value.split_whitespace().collect::<String>();
    let data = base64::engine::general_purpose::STANDARD
        .decode(compact.as_bytes())
        .ok()?;
    Some(ContactPhoto {
        data,
        media_type: photo_media_type(params),
    })
}

fn photo_media_type(params: &[&str]) -> Option<String> {
    params.iter().find_map(|param| {
        let (key, value) = param.split_once('=')?;
        (key.eq_ignore_ascii_case("MEDIATYPE") || key.eq_ignore_ascii_case("TYPE"))
            .then(|| value.trim_matches('"').to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contact_from_vcard_reads_common_fields() {
        let contact = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            Some("etag".to_string()),
            "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nEMAIL;TYPE=work;PREF=1:ADA@EXAMPLE.COM\r\nTEL;TYPE=mobile:+123\r\nORG:Analytical Engines\r\nTITLE:Programmer\r\nNOTE:hello\\nworld\r\nPHOTO;VALUE=URI:https://example.test/a.jpg\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.phones[0].kind.as_deref(), Some("mobile"));
        assert_eq!(
            contact.organizations[0].title.as_deref(),
            Some("Programmer")
        );
        assert_eq!(contact.notes.as_deref(), Some("hello\nworld"));
        assert_eq!(
            contact.photo_url.as_deref(),
            Some("https://example.test/a.jpg")
        );
        assert!(contact.photo.is_none());
    }

    #[test]
    fn contact_from_vcard_reads_inline_photo_data() {
        let contact = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nPHOTO;ENCODING=b;TYPE=JPEG:AQIDBA==\r\nEND:VCARD\r\n",
        );

        let photo = contact.photo.expect("inline photo");
        assert_eq!(photo.data, vec![1, 2, 3, 4]);
        assert_eq!(photo.media_type.as_deref(), Some("JPEG"));
        assert!(contact.photo_url.is_none());
    }

    #[test]
    fn vcard_from_create_escapes_text_fields() {
        let contact = ContactCreate {
            display_name: Some("Ada, Countess".to_string()),
            notes: Some("line 1\r\nline 2\rline 3".to_string()),
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");
        assert!(data.contains("FN:Ada\\, Countess"));
        assert!(data.contains("NOTE:line 1\\nline 2\\nline 3"));
    }

    #[test]
    fn vcard_from_create_writes_postal_addresses() {
        let contact = ContactCreate {
            addresses: vec![ContactAddress {
                kind: Some("home".to_string()),
                formatted: None,
                street: vec!["1 Example St".to_string(), "Unit 2".to_string()],
                locality: Some("London".to_string()),
                region: Some("England".to_string()),
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: true,
            }],
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");

        assert!(data.contains("ADR;TYPE=home;PREF=1:;;1 Example St\\nUnit 2;London;England;N1;UK"));
    }

    #[test]
    fn vcard_from_create_always_emits_fn() {
        let data = vcard_from_create(&ContactCreate::default(), "id-1");
        assert!(data.contains("\r\nFN:\r\n"));
    }

    #[test]
    fn contact_from_vcard_uses_structured_name_when_fn_absent() {
        let contact = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nN:Lovelace;Ada;Byron;Countess;\r\nEMAIL;WORK;PREF:ada@example.test\r\nADR;TYPE=home;PREF=1:;;1 Example St\\nUnit 2;London;England;N1;UK\r\nORG:Acme\\;R&D;Lab\r\nEND:VCARD\r\n",
        );

        assert_eq!(
            contact.display_name.as_deref(),
            Some("Countess Ada Byron Lovelace")
        );
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(contact.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(contact.addresses[0].locality.as_deref(), Some("London"));
        assert!(contact.addresses[0].is_primary);
        assert_eq!(contact.organizations[0].name, "Acme;R&D Lab");
    }

    #[test]
    fn unknown_escapes_are_preserved() {
        let contact = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\\x\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.display_name.as_deref(), Some("Ada\\x"));
    }

    #[test]
    fn vcard_from_create_folds_long_lines_and_quotes_params() {
        let contact = ContactCreate {
            display_name: Some("A".repeat(90)),
            emails: vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("work:main".to_string()),
                is_primary: false,
            }],
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");

        assert!(data.contains("\r\n "));
        assert!(data.lines().all(|line| line.len() <= 75));
        assert!(data.contains("EMAIL;TYPE=\"work:main\":ada@example.test"));
    }

    #[test]
    fn vcard_from_patch_preserves_unmodeled_properties() {
        let current = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            Some("etag".to_string()),
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nN:Lovelace;Ada;;;\r\nBDAY:18151210\r\nX-CUSTOM:value\r\nEMAIL;TYPE=work:old@example.test\r\nEND:VCARD\r\n",
        );
        let patch = ContactPatch {
            emails: Some(vec![ContactEmail {
                value: "new@example.test".to_string(),
                kind: Some("home".to_string()),
                is_primary: true,
            }]),
            ..ContactPatch::default()
        };

        let data = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nN:Lovelace;Ada;;;\r\nBDAY:18151210\r\nX-CUSTOM:value\r\nEMAIL;TYPE=work:old@example.test\r\nEND:VCARD\r\n",
            &patch,
        );

        assert!(data.contains("N:Lovelace;Ada;;;"));
        assert!(data.contains("BDAY:18151210"));
        assert!(data.contains("X-CUSTOM:value"));
        assert!(!data.contains("old@example.test"));
        assert!(data.contains("EMAIL;TYPE=home;PREF=1:new@example.test"));
    }

    #[test]
    fn vcard_from_patch_clears_photo_only_when_requested() {
        let current = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n",
        );

        let unchanged = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n",
            &ContactPatch {
                notes: Some(Some("note".to_string())),
                ..ContactPatch::default()
            },
        );
        assert!(unchanged.contains("PHOTO;ENCODING=b;TYPE=JPEG:abcd"));

        let cleared = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n",
            &ContactPatch {
                photo_url: Some(None),
                ..ContactPatch::default()
            },
        );
        assert!(!cleared.contains("PHOTO;ENCODING=b;TYPE=JPEG"));
    }

    #[test]
    fn vcard_from_patch_writes_inline_photo() {
        let current = contact_from_vcard(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n",
        );

        let updated = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n",
            &ContactPatch {
                photo: Some(Some(ContactPhoto {
                    data: vec![1, 2, 3, 4],
                    media_type: Some("PNG".to_string()),
                })),
                ..ContactPatch::default()
            },
        );

        assert!(updated.contains("PHOTO;ENCODING=b;TYPE=PNG:AQIDBA=="));
        assert!(!updated.contains("PHOTO;ENCODING=b;TYPE=JPEG:abcd"));
    }
}
