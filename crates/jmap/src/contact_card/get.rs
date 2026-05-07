use super::{ContactCard, ContactCardId};

impl ContactCard {
    /// The contact card ID. Returns an owned `ContactCardId` (one
    /// allocation): JSON-map storage means there is no
    /// `&ContactCardId` to borrow.
    pub fn id(&self) -> Option<ContactCardId> {
        self.properties.get("id")?.as_str().map(ContactCardId::from)
    }

    pub fn take_id(&mut self) -> ContactCardId {
        self.properties
            .remove("id")
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(ContactCardId::from(s)),
                _ => None,
            })
            .unwrap_or_else(|| ContactCardId::new(""))
    }

    pub fn uid(&self) -> Option<&str> {
        self.properties.get("uid")?.as_str()
    }

    pub fn address_book_ids(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("addressBookIds")?.as_object()
    }

    pub fn kind(&self) -> Option<&str> {
        self.properties.get("kind")?.as_str()
    }

    pub fn name(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("name")?.as_object()
    }

    pub fn nicknames(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("nicknames")?.as_object()
    }

    pub fn emails(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("emails")?.as_object()
    }

    pub fn phones(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("phones")?.as_object()
    }

    pub fn addresses(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("addresses")?.as_object()
    }

    pub fn organizations(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("organizations")?.as_object()
    }

    pub fn online_services(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("onlineServices")?.as_object()
    }

    pub fn notes(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("notes")?.as_object()
    }

    pub fn media(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.properties.get("media")?.as_object()
    }

    pub fn created(&self) -> Option<&str> {
        self.properties.get("created")?.as_str()
    }

    pub fn updated(&self) -> Option<&str> {
        self.properties.get("updated")?.as_str()
    }

    /// Access any property as a raw JSON value, including extension
    /// properties not covered by the typed accessors.
    pub fn property(&self, name: &str) -> Option<&serde_json::Value> {
        self.properties.get(name)
    }

    /// Access the full underlying JSContact properties map.
    pub fn as_properties(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.properties
    }
}
