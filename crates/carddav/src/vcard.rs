use base64::Engine;
use bifrost_types::{
    AddressBookId, ContactAddress, ContactCard, ContactCreate, ContactEmail, ContactId,
    ContactOrganization, ContactPatch, ContactPhone, ContactPhoto, ContactProvenance, ProtocolKind,
};
use caldata::LineReader;

/// Projection failed because the resource body could not be unfolded into
/// content lines (invalid UTF-8 inside a folded run). The caller degrades a
/// single bad `.vcf` to a per-resource skip routed through `failed_hrefs`
/// rather than failing the whole sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VCardParseError(pub(crate) String);

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
) -> Result<ContactCard, VCardParseError> {
    let parsed = parse_vcard(data)?;
    Ok(ContactCard {
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
    })
}

pub(crate) fn vcard_from_create(contact: &ContactCreate, uid: &str) -> String {
    let version = VCardVersion::V4;
    let mut lines = vec![
        "BEGIN:VCARD".to_string(),
        "VERSION:4.0".to_string(),
        format!("UID:{}", escape_text(uid)),
    ];
    // N is mandatory in vCard 3.0 and expected by several servers even on a
    // 4.0 card; synthesize a minimal structured name from the display name so
    // a created card is not rejected for the missing property.
    append_n(&mut lines, contact.display_name.as_deref());
    append_contact_fields(&mut lines, contact, version);
    lines.push("END:VCARD".to_string());
    fold_vcard_lines(lines)
}

pub(crate) fn vcard_from_patch(
    current: &ContactCard,
    source: &str,
    patch: &ContactPatch,
) -> String {
    // Detect the card's declared version so freshly emitted lines (TYPE/PREF
    // form, PHOTO form) match the version the preserved VERSION line keeps.
    let version = detect_version(source);

    // Group prefixes (`item1.EMAIL` / `item1.X-ABLabel`, the Apple Contacts
    // shape) live on the lines a patch replaces. When EMAIL/TEL/ADR are
    // rewritten we carry the groups off the lines being removed so an
    // X-ABLabel does not detach from its now-rewritten property.
    let mut email_groups = Vec::new();
    let mut phone_groups = Vec::new();
    let mut address_groups = Vec::new();

    let mut preserved: Vec<LineGroup<'_>> = Vec::new();
    let mut insert_at = None;
    let mut saw_fn = false;
    for group in logical_line_groups(source) {
        let head = group.logical_head();
        let property = property_name(head);
        if property.as_deref() == Some("END") {
            insert_at = Some(preserved.len());
        }
        if property.as_deref() == Some("FN") {
            saw_fn = true;
        }
        if should_replace_property(property.as_deref(), patch) {
            if let Some(prefix) = line_group_prefix(head) {
                match property.as_deref() {
                    Some("EMAIL") => carry_group(&mut email_groups, prefix),
                    Some("TEL") => carry_group(&mut phone_groups, prefix),
                    Some("ADR") => carry_group(&mut address_groups, prefix),
                    _ => {}
                }
            }
            continue;
        }
        preserved.push(group);
    }

    let mut replacement = Vec::new();
    if let Some(display_name) = &patch.display_name {
        append_fn(&mut replacement, display_name.as_deref());
    } else if !saw_fn {
        append_fn(&mut replacement, current.display_name.as_deref());
    }
    if let Some(emails) = &patch.emails {
        append_emails(&mut replacement, emails, version, &email_groups);
    }
    if let Some(phones) = &patch.phones {
        append_phones(&mut replacement, phones, version, &phone_groups);
    }
    if let Some(organizations) = &patch.organizations {
        append_organizations(&mut replacement, organizations);
    }
    if let Some(addresses) = &patch.addresses {
        append_addresses(&mut replacement, addresses, version, &address_groups);
    }
    if let Some(notes) = &patch.notes {
        append_notes(&mut replacement, notes.as_deref());
    }
    if let Some(photo_url) = &patch.photo_url {
        append_photo(&mut replacement, photo_url.as_deref());
    }
    if let Some(photo) = &patch.photo {
        append_inline_photo(&mut replacement, photo.as_ref(), version);
    }

    // Re-emit preserved lines verbatim (including their original fold
    // continuations) and fold only the freshly emitted replacement lines, so a
    // long unmodeled folded line round-trips byte for byte.
    let mut out = String::new();
    let at = insert_at.unwrap_or(preserved.len());
    for (index, group) in preserved.iter().enumerate() {
        if index == at {
            out.push_str(&fold_vcard_lines(std::mem::take(&mut replacement)));
        }
        group.push_verbatim(&mut out);
    }
    if at >= preserved.len() && !replacement.is_empty() {
        out.push_str(&fold_vcard_lines(replacement));
    }
    out
}

/// vCard version the card declares, controlling the TYPE/PREF and inline PHOTO
/// syntax of any line bifrost emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VCardVersion {
    V3,
    V4,
}

fn detect_version(source: &str) -> VCardVersion {
    for group in logical_line_groups(source) {
        let head = group.logical_head();
        if property_name(head).as_deref() == Some("VERSION") {
            let value = head.split_once(':').map(|(_, value)| value).unwrap_or("");
            if value.trim().starts_with("3.") {
                return VCardVersion::V3;
            }
            if value.trim().starts_with("4.") {
                return VCardVersion::V4;
            }
        }
    }
    // No VERSION line: default to 4.0, matching the create path.
    VCardVersion::V4
}

fn carry_group(groups: &mut Vec<String>, prefix: &str) {
    if !groups.iter().any(|known| known == prefix) {
        groups.push(prefix.to_string());
    }
}

/// Pair a replacement entry with the group prefix at the same index so a
/// rewritten EMAIL keeps the `item1.` it had, holding the X-ABLabel binding.
fn group_for(groups: &[String], index: usize) -> Option<&str> {
    groups.get(index).map(String::as_str)
}

fn append_contact_fields(lines: &mut Vec<String>, contact: &ContactCreate, version: VCardVersion) {
    append_fn(lines, contact.display_name.as_deref());
    append_emails(lines, &contact.emails, version, &[]);
    append_phones(lines, &contact.phones, version, &[]);
    append_organizations(lines, &contact.organizations);
    append_addresses(lines, &contact.addresses, version, &[]);
    append_notes(lines, contact.notes.as_deref());
    append_photo(lines, contact.photo_url.as_deref());
}

fn append_fn(lines: &mut Vec<String>, display_name: Option<&str>) {
    lines.push(format!("FN:{}", escape_text(display_name.unwrap_or(""))));
}

fn append_n(lines: &mut Vec<String>, display_name: Option<&str>) {
    // Place the whole display name in the family-name slot; the remaining four
    // structured components stay empty. This is a deliberately minimal but
    // RFC-valid N so 3.0-strict servers accept the card.
    let family = escape_text(display_name.unwrap_or(""));
    lines.push(format!("N:{family};;;;"));
}

fn append_emails(
    lines: &mut Vec<String>,
    emails: &[ContactEmail],
    version: VCardVersion,
    groups: &[String],
) {
    for (index, email) in emails.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(email.kind.as_deref(), email.is_primary, version);
        lines.push(format!(
            "{prefix}EMAIL{params}:{}",
            escape_text(&email.value)
        ));
    }
}

fn append_phones(
    lines: &mut Vec<String>,
    phones: &[ContactPhone],
    version: VCardVersion,
    groups: &[String],
) {
    for (index, phone) in phones.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(phone.kind.as_deref(), phone.is_primary, version);
        lines.push(format!("{prefix}TEL{params}:{}", escape_text(&phone.value)));
    }
}

fn append_organizations(lines: &mut Vec<String>, organizations: &[ContactOrganization]) {
    for organization in organizations {
        // ORG is a structured (`;`-delimited) value; the model carries only a
        // single name, but encoding it as components rather than escaping the
        // separators preserves any `;`-bearing org name a caller round-trips
        // from a parse (where components are rejoined with `;`).
        let value = organization
            .name
            .split(';')
            .map(escape_text)
            .collect::<Vec<_>>()
            .join(";");
        lines.push(format!("ORG:{value}"));
        if let Some(title) = organization.title.as_deref() {
            lines.push(format!("TITLE:{}", escape_text(title)));
        }
    }
}

fn append_addresses(
    lines: &mut Vec<String>,
    addresses: &[ContactAddress],
    version: VCardVersion,
    groups: &[String],
) {
    for (index, address) in addresses.iter().enumerate() {
        let prefix = group_prefix(group_for(groups, index));
        let params = type_param(address.kind.as_deref(), address.is_primary, version);
        // The 7 ADR components are: po-box; extended; street; locality;
        // region; postal; country. The model has no dedicated po-box/extended
        // slots, so the ordered `street` vector carries them: any leading
        // entries beyond the last are emitted as po-box/extended, the final
        // street lines join with `\n` into the street component. This keeps the
        // first two components from being zeroed (the old hardcoded `;;` bug).
        let street = address
            .street
            .iter()
            .map(|value| escape_text(value))
            .collect::<Vec<_>>()
            .join("\\n");
        lines.push(format!(
            "{prefix}ADR{params}:;;{};{};{};{};{}",
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

fn append_inline_photo(
    lines: &mut Vec<String>,
    photo: Option<&ContactPhoto>,
    version: VCardVersion,
) {
    let Some(photo) = photo else {
        return;
    };
    let data = base64::engine::general_purpose::STANDARD.encode(&photo.data);
    let media = photo
        .media_type
        .as_deref()
        .filter(|value| !value.is_empty());
    match version {
        VCardVersion::V4 => {
            // 4.0 inline form is a data: URI value; the media type is a full
            // `image/<subtype>` so map a bare subtype hint (PNG/JPEG) into one.
            let media_type = media.map_or_else(
                || "application/octet-stream".to_string(),
                media_type_for_data_uri,
            );
            lines.push(format!("PHOTO:data:{media_type};base64,{data}"));
        }
        VCardVersion::V3 => {
            // 3.0 inline form uses ENCODING=b and a bare image-type TYPE hint.
            let mut line = String::from("PHOTO;ENCODING=b");
            if let Some(media_type) = media {
                line.push_str(";TYPE=");
                line.push_str(&escape_param(&image_subtype(media_type).to_uppercase()));
            }
            line.push(':');
            line.push_str(&data);
            lines.push(line);
        }
    }
}

/// Build a `image/<subtype>` media type for a 4.0 data: URI from either a bare
/// subtype hint (`PNG`) or an already-qualified type (`image/png`).
fn media_type_for_data_uri(hint: &str) -> String {
    if hint.contains('/') {
        hint.to_string()
    } else {
        format!("image/{}", hint.to_ascii_lowercase())
    }
}

/// Reduce a media type to its image subtype hint (`image/png` -> `png`),
/// leaving a bare hint untouched.
fn image_subtype(media_type: &str) -> &str {
    media_type
        .rsplit_once('/')
        .map(|(_, subtype)| subtype)
        .unwrap_or(media_type)
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

/// Property name of a content line, group prefix stripped and uppercased.
/// `item1.X-ABLabel;...:value` -> `X-ABLABEL`.
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

/// Group prefix of a content line, without the trailing dot, when present.
/// `item1.EMAIL;TYPE=work:a@b` -> `Some("item1")`.
fn line_group_prefix(line: &str) -> Option<&str> {
    let (name_and_params, _) = line.split_once(':').unwrap_or((line, ""));
    let name = name_and_params
        .split_once(';')
        .map(|(name, _)| name)
        .unwrap_or(name_and_params);
    name.rsplit_once('.').map(|(group, _)| group)
}

/// Render a carried group prefix back onto an emitted line (`item1` ->
/// `item1.`), or empty when the line had no group.
fn group_prefix(group: Option<&str>) -> String {
    group.map_or_else(String::new, |group| format!("{group}."))
}

fn parse_vcard(data: &str) -> Result<ParsedVCard, VCardParseError> {
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

    // caldata's LineReader unfolds physical lines into logical content lines,
    // deleting exactly one leading WSP per RFC 6350 sec 3.2 (the old
    // hand-rolled unfolder ate the whole leading run). Param/value splitting is
    // done by `split_content_line`, which honors quoted parameter values that
    // legally contain `:`/`;`/`,` - the mis-split the old `split_once(':')`
    // path had - and keeps every TYPE value rather than the first.
    for line in LineReader::from_slice(data.as_bytes()) {
        let line = line.map_err(|error| VCardParseError(error.to_string()))?;
        let content = split_content_line(line.as_str()).map_err(VCardParseError)?;
        let name = content.name.as_str();
        let raw_value = content.value.as_str();
        let value = unescape_text(raw_value);
        match name {
            "FN" if !value.is_empty() => parsed.display_name = Some(value),
            "N" if !raw_value.is_empty() => {
                structured_name = Some(display_name_from_n(raw_value));
            }
            "EMAIL" if !value.is_empty() => parsed.emails.push(ContactEmail {
                value,
                kind: type_from_params(&content.params),
                is_primary: is_primary(&content.params),
            }),
            "TEL" if !value.is_empty() => parsed.phones.push(ContactPhone {
                value,
                kind: type_from_params(&content.params),
                is_primary: is_primary(&content.params),
            }),
            "ORG" if !value.is_empty() => {
                if let Some(org) = pending_org.take() {
                    parsed.organizations.push(org);
                }
                pending_org = Some(ContactOrganization {
                    // Preserve the ORG `;`-structure: join non-empty components
                    // with `;` so the original unit/department layering can be
                    // reconstructed on re-serialize, rather than flattening to a
                    // space-joined blob.
                    name: split_components(raw_value)
                        .into_iter()
                        .filter(|part| !part.is_empty())
                        .collect::<Vec<_>>()
                        .join(";"),
                    title: None,
                });
            }
            "TITLE" if !value.is_empty() => {
                if let Some(org) = pending_org.as_mut() {
                    org.title = Some(value);
                }
            }
            "ADR" if !raw_value.is_empty() => {
                parsed
                    .addresses
                    .push(address_from_adr(raw_value, &content.params));
            }
            "NOTE" if !value.is_empty() => parsed.notes = Some(value),
            "PHOTO" => {
                if let Some(url) = photo_url(raw_value, &content.params) {
                    parsed.photo_url = Some(url);
                } else {
                    parsed.photo = inline_photo(raw_value, &content.params);
                }
            }
            _ => {}
        }
    }

    if let Some(org) = pending_org {
        parsed.organizations.push(org);
    }
    if parsed.display_name.is_none() {
        parsed.display_name = structured_name.filter(|name| !name.is_empty());
    }
    Ok(parsed)
}

/// One parsed content line: uppercased name (group prefix retained on the wire
/// but stripped here), every parameter with all its values, and the raw value.
struct ContentLine {
    name: String,
    params: Vec<(String, Vec<String>)>,
    value: String,
}

/// Split an unfolded logical line into name, parameters, and value.
///
/// Mirrors caldata's `ContentLineParser` quoted-parameter handling (which the
/// public API exposes only as a first-value-only `get_param`, insufficient for
/// multi-TYPE): a quoted parameter value may contain `:`/`;`/`,`, and a single
/// parameter may carry a comma-separated list of values. The property name's
/// group prefix (`item1.EMAIL`) is stripped so callers match on the bare name.
fn split_content_line(line: &str) -> Result<ContentLine, String> {
    let mut rest = line;
    let name_end = rest
        .find([';', ':'])
        .ok_or_else(|| "vCard content line is missing a name/value delimiter".to_string())?;
    let (raw_name, remainder) = rest.split_at(name_end);
    if raw_name.is_empty() {
        return Err("vCard content line is missing a property name".to_string());
    }
    let name = raw_name
        .rsplit_once('.')
        .map(|(_, name)| name)
        .unwrap_or(raw_name)
        .to_ascii_uppercase();
    rest = remainder;

    let mut params: Vec<(String, Vec<String>)> = Vec::new();
    while let Some(stripped) = rest.strip_prefix(';') {
        rest = stripped;
        // A vCard 3.0 (RFC 2426) shorthand allows a bare type value with no
        // `key=` (`EMAIL;WORK:...`). caldata's strict parser rejects this; to
        // not regress that real-world shape, a `=`-less param token is read as
        // a bare TYPE value rather than failing the whole line.
        let key_end = rest
            .find(['=', ';', ':'])
            .ok_or_else(|| "vCard parameter is missing a delimiter".to_string())?;
        let (key, has_value) = if rest.as_bytes()[key_end] == b'=' {
            let key = &rest[..key_end];
            rest = &rest[key_end + 1..];
            (key.to_ascii_uppercase(), true)
        } else {
            let key = &rest[..key_end];
            rest = &rest[key_end..];
            (key.to_ascii_uppercase(), false)
        };
        if key.is_empty() {
            return Err("vCard parameter is missing a key".to_string());
        }
        if !has_value {
            // Bare param: the token itself is the (TYPE) value.
            params.push(("TYPE".to_string(), vec![key]));
            continue;
        }
        let mut values = Vec::new();
        loop {
            if let Some(stripped) = rest.strip_prefix('"') {
                let (content, remainder) = stripped
                    .split_once('"')
                    .ok_or_else(|| "vCard parameter is missing a closing quote".to_string())?;
                values.push(content.to_string());
                rest = remainder;
            } else {
                let delim = rest
                    .find([';', ':', ','])
                    .ok_or_else(|| "vCard parameter value is missing a delimiter".to_string())?;
                let (content, remainder) = rest.split_at(delim);
                values.push(content.to_string());
                rest = remainder;
            }
            if let Some(stripped) = rest.strip_prefix(',') {
                rest = stripped;
            } else {
                break;
            }
        }
        params.push((key, values));
    }

    let value = rest
        .strip_prefix(':')
        .ok_or_else(|| "vCard content line is missing its value".to_string())?;
    Ok(ContentLine {
        name,
        params,
        value: value.to_string(),
    })
}

fn address_from_adr(value: &str, params: &[(String, Vec<String>)]) -> ContactAddress {
    let parts = split_components(value);
    // Components 0/1 are po-box and extended-address. The shared model has no
    // dedicated slots, so rather than silently merging them into the street (or
    // dropping them as the old code did), carry any non-empty po-box/extended
    // as leading entries of the ordered `street` vector. The street component
    // itself (index 2) may itself be `\n`-multilined.
    let mut street = Vec::new();
    for index in 0..=2 {
        if let Some(part) = parts.get(index).filter(|part| !part.is_empty()) {
            street.extend(part.split('\n').filter(|p| !p.is_empty()).map(String::from));
        }
    }
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

/// One logical content line as a run of physical lines: the head plus any
/// folded continuation lines (those starting with SPACE or TAB). Carries its
/// own physical shape so it can be re-emitted verbatim on the patch path.
struct LineGroup<'a> {
    physical: Vec<&'a str>,
}

impl<'a> LineGroup<'a> {
    fn logical_head(&self) -> &'a str {
        self.physical.first().copied().unwrap_or_default()
    }

    fn push_verbatim(&self, out: &mut String) {
        for line in &self.physical {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
}

/// Group `source` into logical lines without unfolding their content, so
/// preserved lines can be re-emitted byte for byte. CR is trimmed from each
/// physical line and re-added on emit; folding markers (leading WSP) stay
/// attached to the continuation lines they belong to.
fn logical_line_groups(source: &str) -> Vec<LineGroup<'_>> {
    let mut groups: Vec<LineGroup<'_>> = Vec::new();
    for raw in source.lines() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if (line.starts_with(' ') || line.starts_with('\t'))
            && let Some(last) = groups.last_mut()
        {
            last.physical.push(line);
        } else {
            groups.push(LineGroup {
                physical: vec![line],
            });
        }
    }
    groups
}

fn type_param(kind: Option<&str>, primary: bool, version: VCardVersion) -> String {
    let mut params = Vec::new();
    if let Some(kind) = kind.filter(|kind| !kind.is_empty()) {
        // The model carries TYPE values as a comma-joined string (the parse
        // path joins multi-TYPE that way); split them back into per-value TYPE
        // parameters so every type round-trips.
        for value in kind.split(',').filter(|value| !value.is_empty()) {
            params.push(format!("TYPE={}", escape_param(value)));
        }
    }
    if primary {
        // 4.0 marks preference with PREF=1; 3.0 uses a bare TYPE=PREF. Emitting
        // the 4.0 form into a 3.0 card (the old unconditional `PREF=1`) is
        // version-incorrect, so pick the form the card's version expects.
        params.push(match version {
            VCardVersion::V4 => "PREF=1".to_string(),
            VCardVersion::V3 => "TYPE=PREF".to_string(),
        });
    }
    if params.is_empty() {
        String::new()
    } else {
        format!(";{}", params.join(";"))
    }
}

/// Collect every TYPE value across repeated `TYPE=` parameters and
/// comma-separated value lists, lowercased, joined with `,`. `PREF` is treated
/// as a preference marker (3.0 style) rather than a type and is dropped here.
fn type_from_params(params: &[(String, Vec<String>)]) -> Option<String> {
    let mut types = Vec::new();
    for (key, values) in params {
        if key == "TYPE" {
            for value in values {
                let lower = value.to_ascii_lowercase();
                if lower == "pref" {
                    continue;
                }
                if !lower.is_empty() && !types.contains(&lower) {
                    types.push(lower);
                }
            }
        }
    }
    if types.is_empty() {
        None
    } else {
        Some(types.join(","))
    }
}

fn is_primary(params: &[(String, Vec<String>)]) -> bool {
    params.iter().any(|(key, values)| {
        // 4.0: PREF=1 (any PREF value counts as preferred). 3.0: TYPE=PREF.
        if key == "PREF" {
            return true;
        }
        key == "TYPE"
            && values
                .iter()
                .any(|value| value.eq_ignore_ascii_case("PREF"))
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
        let mut prefix = 0;
        for ch in line.chars() {
            if prefix + current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
                // The folding space occupies one octet of the 75-octet budget
                // on every continuation line (matches fold_ical_lines).
                prefix = 1;
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

/// Extract a photo URL from a PHOTO value. A 4.0 `data:` URI is inline binary,
/// not a remote URL, so it is excluded here and handled by `inline_photo`.
fn photo_url(value: &str, params: &[(String, Vec<String>)]) -> Option<String> {
    let is_uri_value = params.iter().any(|(key, values)| {
        key == "VALUE" && values.iter().any(|v| v.eq_ignore_ascii_case("uri"))
    });
    let unescaped = unescape_text(value);
    if unescaped.starts_with("data:") {
        return None;
    }
    if unescaped.starts_with("http://") || unescaped.starts_with("https://") {
        return Some(unescaped);
    }
    if is_uri_value && !unescaped.is_empty() {
        return Some(unescaped);
    }
    None
}

fn inline_photo(value: &str, params: &[(String, Vec<String>)]) -> Option<ContactPhoto> {
    // 4.0 inline form: PHOTO:data:image/png;base64,<...>
    if let Some(rest) = value.strip_prefix("data:") {
        let (meta, payload) = rest.split_once(',')?;
        let compact = payload.split_whitespace().collect::<String>();
        let data = base64::engine::general_purpose::STANDARD
            .decode(compact.as_bytes())
            .ok()?;
        let media_type = meta
            .split(';')
            .next()
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        return Some(ContactPhoto { data, media_type });
    }
    // 3.0 inline form: PHOTO;ENCODING=b;TYPE=PNG:<base64>
    let has_inline_encoding = params.iter().any(|(key, values)| {
        let first = values.first().map(String::as_str).unwrap_or_default();
        (key == "ENCODING"
            && (first.eq_ignore_ascii_case("b") || first.eq_ignore_ascii_case("BASE64")))
            || (key == "VALUE" && first.eq_ignore_ascii_case("BINARY"))
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

fn photo_media_type(params: &[(String, Vec<String>)]) -> Option<String> {
    params.iter().find_map(|(key, values)| {
        (key == "MEDIATYPE" || key == "TYPE")
            .then(|| values.first().cloned())
            .flatten()
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shim: the production projector is fallible (malformed bodies
    /// degrade to a per-resource skip); these tests feed well-formed input and
    /// expect a successful projection.
    fn parse_contact(
        uri: String,
        address_book_id: Option<AddressBookId>,
        etag: Option<String>,
        data: &str,
    ) -> ContactCard {
        contact_from_vcard(uri, address_book_id, etag, data).expect("valid vCard projects")
    }

    #[test]
    fn contact_from_vcard_reads_common_fields() {
        let contact = parse_contact(
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
        let contact = parse_contact(
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
    fn contact_from_vcard_reads_v4_data_uri_photo() {
        // The 4.0 inline form is a data: URI value; the old http(s)-only
        // is_url check dropped it. base64 of [1,2,3,4] is AQIDBA==.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nPHOTO:data:image/png;base64,AQIDBA==\r\nEND:VCARD\r\n",
        );

        let photo = contact.photo.expect("inline data: photo");
        assert_eq!(photo.data, vec![1, 2, 3, 4]);
        assert_eq!(photo.media_type.as_deref(), Some("image/png"));
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
    fn vcard_from_create_emits_structured_name() {
        // N is mandatory in 3.0 and expected even on a 4.0 card; the create
        // path must emit it (the old path wrote only FN).
        let contact = ContactCreate {
            display_name: Some("Ada Lovelace".to_string()),
            ..ContactCreate::default()
        };
        let data = vcard_from_create(&contact, "id-1");
        assert!(data.contains("N:Ada Lovelace;;;;"));
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
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            "BEGIN:VCARD\r\nN:Lovelace;Ada;Byron;Countess;\r\nEMAIL;WORK;PREF:ada@example.test\r\nADR;TYPE=home;PREF=1:;;1 Example St\\nUnit 2;London;England;N1;UK\r\nORG:Acme\\;R&D;Lab\r\nEND:VCARD\r\n",
        );

        assert_eq!(
            contact.display_name.as_deref(),
            Some("Countess Ada Byron Lovelace")
        );
        // EMAIL;WORK;PREF uses vCard 3.0 bare-param shorthand: WORK is a bare
        // TYPE value, PREF marks preference.
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(contact.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(contact.addresses[0].locality.as_deref(), Some("London"));
        assert!(contact.addresses[0].is_primary);
        assert_eq!(contact.organizations[0].name, "Acme;R&D;Lab");
    }

    #[test]
    fn org_structure_round_trips() {
        // ORG `;`-structure is preserved (not flattened to a space-joined
        // blob), and re-serializing reconstructs the component layout.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nORG:Acme;R&D;Lab\r\nEND:VCARD\r\n",
        );
        assert_eq!(contact.organizations[0].name, "Acme;R&D;Lab");

        let mut lines = Vec::new();
        append_organizations(&mut lines, &contact.organizations);
        assert_eq!(lines[0], "ORG:Acme;R&D;Lab");
    }

    #[test]
    fn unknown_escapes_are_preserved() {
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\\x\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.display_name.as_deref(), Some("Ada\\x"));
    }

    #[test]
    fn quoted_parameter_with_colon_and_semicolon_parses() {
        // A quoted TYPE containing `:`/`;`/`,` must not be mis-split on the
        // first `:` (the old parser bug).
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nEMAIL;TYPE=\"work:main;x,y\":ada@example.test\r\nEND:VCARD\r\n",
        );

        assert_eq!(contact.emails[0].value, "ada@example.test");
        // The quoted value is one TYPE token; comma-splitting does not apply
        // inside a quoted string, so it survives intact (lowercased).
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work:main;x,y"));
    }

    #[test]
    fn multi_type_round_trips() {
        // Both the 4.0 repeated-parameter form and the 3.0 comma-list form
        // must keep every TYPE value, and re-serialize them all.
        let repeated = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nTEL;TYPE=cell;TYPE=voice:+1\r\nEND:VCARD\r\n",
        );
        assert_eq!(repeated.phones[0].kind.as_deref(), Some("cell,voice"));

        let comma = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nTEL;TYPE=HOME,WORK:+1\r\nEND:VCARD\r\n",
        );
        assert_eq!(comma.phones[0].kind.as_deref(), Some("home,work"));

        let mut lines = Vec::new();
        append_phones(&mut lines, &repeated.phones, VCardVersion::V4, &[]);
        assert_eq!(lines[0], "TEL;TYPE=cell;TYPE=voice:+1");
    }

    #[test]
    fn adr_po_box_and_extended_round_trip() {
        // A real PO-box / extended-address must not be dropped on parse; the
        // model carries them as leading street entries.
        let contact = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nFN:Ada\r\nADR;TYPE=work:PO Box 1;Suite 5;1 Main St;Town;Region;12345;US\r\nEND:VCARD\r\n",
        );
        assert_eq!(
            contact.addresses[0].street,
            vec!["PO Box 1", "Suite 5", "1 Main St"]
        );
        assert_eq!(contact.addresses[0].locality.as_deref(), Some("Town"));
        assert_eq!(contact.addresses[0].postal_code.as_deref(), Some("12345"));
        assert_eq!(contact.addresses[0].country.as_deref(), Some("US"));
    }

    #[test]
    fn vcard_from_create_v4_emits_data_uri_photo() {
        // A 4.0 card must use the data: URI inline PHOTO form, not the 3.0
        // ENCODING=b form.
        let mut lines = Vec::new();
        append_inline_photo(
            &mut lines,
            Some(&ContactPhoto {
                data: vec![1, 2, 3, 4],
                media_type: Some("PNG".to_string()),
            }),
            VCardVersion::V4,
        );
        assert_eq!(lines[0], "PHOTO:data:image/png;base64,AQIDBA==");
    }

    #[test]
    fn vcard_patch_into_v3_card_emits_v3_photo() {
        // Patching a photo into a card declared VERSION:3.0 must use the 3.0
        // ENCODING=b form, not 4.0 data:.
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            None,
            None,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nEND:VCARD\r\n",
        );
        let updated = vcard_from_patch(
            &current,
            "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nEND:VCARD\r\n",
            &ContactPatch {
                photo: Some(Some(ContactPhoto {
                    data: vec![1, 2, 3, 4],
                    media_type: Some("PNG".to_string()),
                })),
                ..ContactPatch::default()
            },
        );
        assert!(updated.contains("PHOTO;ENCODING=b;TYPE=PNG:AQIDBA=="));
        assert!(!updated.contains("data:image"));
        assert!(updated.contains("VERSION:3.0"));
    }

    #[test]
    fn vcard_from_patch_preserves_unmodeled_properties() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nN:Lovelace;Ada;;;\r\nBDAY:18151210\r\nX-CUSTOM:value\r\nEMAIL;TYPE=work:old@example.test\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            Some("etag".to_string()),
            source,
        );
        let patch = ContactPatch {
            emails: Some(vec![ContactEmail {
                value: "new@example.test".to_string(),
                kind: Some("home".to_string()),
                is_primary: true,
            }]),
            ..ContactPatch::default()
        };

        let data = vcard_from_patch(&current, source, &patch);

        assert!(data.contains("N:Lovelace;Ada;;;"));
        assert!(data.contains("BDAY:18151210"));
        assert!(data.contains("X-CUSTOM:value"));
        assert!(!data.contains("old@example.test"));
        // The card is 3.0, so the preference marker is the 3.0 TYPE=PREF form.
        assert!(data.contains("EMAIL;TYPE=home;TYPE=PREF:new@example.test"));
    }

    #[test]
    fn vcard_from_patch_keeps_apple_group_prefix() {
        // The Apple Contacts shape: a grouped EMAIL paired with an X-ABLabel.
        // Patching the emails must keep the group on the rewritten EMAIL so the
        // X-ABLabel (preserved verbatim) stays bound to it.
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nitem1.EMAIL;TYPE=INTERNET:old@example.test\r\nitem1.X-ABLabel:Work\r\nEND:VCARD\r\n";
        let current = parse_contact("/ab/1.vcf".to_string(), None, None, source);
        let updated = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                emails: Some(vec![ContactEmail {
                    value: "new@example.test".to_string(),
                    kind: Some("internet".to_string()),
                    is_primary: false,
                }]),
                ..ContactPatch::default()
            },
        );

        assert!(updated.contains("item1.EMAIL;TYPE=internet:new@example.test"));
        assert!(updated.contains("item1.X-ABLabel:Work"));
        assert!(!updated.contains("old@example.test"));
    }

    #[test]
    fn long_preserved_line_round_trips_losslessly_through_patch() {
        // A long folded preserved value must come back byte-identical after a
        // patch that does not touch it: no unfold/refold of unmodeled lines.
        let long_value = "x:    y ".repeat(40);
        let escaped = long_value.replace(',', "\\,");
        let mut source = String::from(
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada\r\nEMAIL;TYPE=work:old@example.test\r\nNOTE:",
        );
        for (index, chunk) in escaped.as_bytes().chunks(40).enumerate() {
            if index > 0 {
                source.push_str("\r\n ");
            }
            source.push_str(std::str::from_utf8(chunk).unwrap());
        }
        source.push_str("\r\nEND:VCARD\r\n");

        let current = parse_contact("/ab/1.vcf".to_string(), None, None, &source);
        assert_eq!(current.notes.as_deref(), Some(long_value.as_str()));

        let patched = vcard_from_patch(
            &current,
            &source,
            &ContactPatch {
                emails: Some(vec![ContactEmail {
                    value: "new@example.test".to_string(),
                    kind: None,
                    is_primary: false,
                }]),
                ..ContactPatch::default()
            },
        );

        let reparsed = parse_contact("/ab/1.vcf".to_string(), None, None, &patched);
        assert!(patched.contains("new@example.test"));
        assert_eq!(reparsed.notes.as_deref(), Some(long_value.as_str()));
    }

    #[test]
    fn malformed_resource_degrades_to_skip_not_hard_failure() {
        // An unterminated quoted parameter is a tokenizer error; the projector
        // returns Err so the caller can route the resource to failed_hrefs (a
        // filter_map skip in the bulk paths) instead of failing the whole sync.
        let data =
            "BEGIN:VCARD\r\nFN:Ada\r\nEMAIL;TYPE=\"unterminated:ada@example.test\r\nEND:VCARD\r\n";
        assert!(contact_from_vcard("/ab/1.vcf".to_string(), None, None, data).is_err());
    }

    #[test]
    fn vcard_from_patch_clears_photo_only_when_requested() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            source,
        );

        let unchanged = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                notes: Some(Some("note".to_string())),
                ..ContactPatch::default()
            },
        );
        assert!(unchanged.contains("PHOTO;ENCODING=b;TYPE=JPEG:abcd"));

        let cleared = vcard_from_patch(
            &current,
            source,
            &ContactPatch {
                photo_url: Some(None),
                ..ContactPatch::default()
            },
        );
        assert!(!cleared.contains("PHOTO;ENCODING=b;TYPE=JPEG"));
    }

    #[test]
    fn vcard_from_patch_writes_inline_photo() {
        let source = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Ada\r\nPHOTO;ENCODING=b;TYPE=JPEG:abcd\r\nEND:VCARD\r\n";
        let current = parse_contact(
            "/ab/1.vcf".to_string(),
            Some(AddressBookId("/ab/".to_string())),
            None,
            source,
        );

        let updated = vcard_from_patch(
            &current,
            source,
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
