use serde::{Serialize, de::DeserializeOwned};

use super::capability::Capability;

/// A self-describing JMAP method call.
///
/// Each JMAP method (Email/get, CalendarEvent/query, etc.) is a concrete
/// type implementing this trait. The trait carries the wire name,
/// required capability, and associated response type.
pub trait JmapMethod: Serialize + Send {
    /// Wire method name, e.g. `"Email/get"`.
    const NAME: &'static str;

    /// The capability required for this method.
    type Cap: Capability;

    /// The deserialized response type.
    type Response: DeserializeOwned + Send;

    /// Inject the request's account ID into this method's `accountId`
    /// field just before serialization.
    ///
    /// Called by [`crate::core::request::Request::call`] (and through
    /// it by [`crate::Account::call`]) so callers no longer pass the
    /// account ID through every method-struct constructor. The default
    /// no-op suits methods that do not carry an `accountId` (e.g.
    /// `Core/echo`); account-scoped methods override it.
    ///
    /// Cross-account methods (`Email/copy`) only inject the
    /// destination here; the source `fromAccountId` is supplied at
    /// construction time.
    fn set_account_id(&mut self, _account_id: &str) {}
}

/// Generates a JMAP /get method struct that wraps `GetRequest<O>`.
///
/// The macro emits value-builder forwarders (`fn ids(self, ...) -> Self`)
/// on the outer struct so callers can chain
/// `EmailGet::new().ids([id]).properties([...])`. The inner
/// `GetRequest`'s `&mut self` setters stay as-is for internal use; the
/// outer simply delegates and returns `Self` by value.
#[macro_export]
macro_rules! define_get_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::get::GetRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    inner: $crate::core::get::GetRequest::new(),
                }
            }

            #[must_use]
            pub fn ids<U, V>(mut self, ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<String>,
            {
                self.inner.ids(ids);
                self
            }

            #[must_use]
            pub fn ids_ref(mut self, reference: $crate::core::request::ResultReference) -> Self {
                self.inner.ids_ref(reference);
                self
            }

            #[must_use]
            pub fn properties(
                mut self,
                properties: impl IntoIterator<Item = <$obj as $crate::core::Object>::Property>,
            ) -> Self {
                self.inner.properties(properties);
                self
            }

            #[must_use]
            pub fn properties_ref(
                mut self,
                reference: $crate::core::request::ResultReference,
            ) -> Self {
                self.inner.properties_ref(reference);
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::get::GetRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /set method struct that wraps `SetRequest<O>`.
///
/// `create()` and `update()` accessors return `&mut entry` so callers
/// can populate or patch entries in place; those stay imperative even
/// in the value-builder world.
#[macro_export]
macro_rules! define_set_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::set::SetRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    inner: $crate::core::set::SetRequest::new(),
                }
            }

            #[must_use]
            pub fn if_in_state(mut self, if_in_state: impl Into<String>) -> Self {
                self.inner.if_in_state(if_in_state);
                self
            }

            #[must_use]
            pub fn destroy<U, V>(mut self, ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<String>,
            {
                self.inner.destroy(ids);
                self
            }

            #[must_use]
            pub fn destroy_ref(
                mut self,
                reference: $crate::core::request::ResultReference,
            ) -> Self {
                self.inner.destroy_ref(reference);
                self
            }

            // `create` / `update` are not lifted onto the outer here:
            // they only exist for objects that impl
            // `SetObjectCreatable`, and Rust resolves their existence
            // at impl-block expansion time. Callers reach them via
            // Deref/DerefMut into the inner SetRequest, which keeps
            // the conditional-impl shape intact and avoids generating
            // dead methods on non-creatable Set types like
            // ShareNotificationSet.
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::set::SetRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /changes method struct that wraps `ChangesRequest`.
#[macro_export]
macro_rules! define_changes_method {
    ($name:ident, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::changes::ChangesRequest,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new(since_state: impl Into<String>) -> Self {
                Self {
                    inner: $crate::core::changes::ChangesRequest::new(since_state),
                }
            }

            #[must_use]
            pub fn max_changes(mut self, max_changes: std::num::NonZeroUsize) -> Self {
                self.inner.max_changes(max_changes);
                self
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::changes::ChangesRequest;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /query method struct that wraps `QueryRequest<O>`.
#[macro_export]
macro_rules! define_query_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::query::QueryRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $crate::core::query::QueryResponse;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    inner: $crate::core::query::QueryRequest::new(),
                }
            }

            #[must_use]
            pub fn filter(
                mut self,
                filter: impl Into<
                    $crate::core::query::Filter<<$obj as $crate::core::query::QueryObject>::Filter>,
                >,
            ) -> Self {
                self.inner.filter(filter);
                self
            }

            #[must_use]
            pub fn sort(
                mut self,
                sort: impl IntoIterator<
                    Item = $crate::core::query::Comparator<
                        <$obj as $crate::core::query::QueryObject>::Sort,
                    >,
                >,
            ) -> Self {
                self.inner.sort(sort);
                self
            }

            #[must_use]
            pub fn position(mut self, position: i32) -> Self {
                self.inner.position(position);
                self
            }

            #[must_use]
            pub fn anchor(mut self, anchor: impl Into<String>) -> Self {
                self.inner.anchor(anchor);
                self
            }

            #[must_use]
            pub fn anchor_offset(mut self, anchor_offset: i32) -> Self {
                self.inner.anchor_offset(anchor_offset);
                self
            }

            #[must_use]
            pub fn limit(mut self, limit: usize) -> Self {
                self.inner.limit(limit);
                self
            }

            #[must_use]
            pub fn calculate_total(mut self, calculate_total: bool) -> Self {
                self.inner.calculate_total(calculate_total);
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::query::QueryRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /queryChanges method struct.
#[macro_export]
macro_rules! define_query_changes_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::query_changes::QueryChangesRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $crate::core::query_changes::QueryChangesResponse;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new(since_query_state: impl Into<String>) -> Self {
                Self {
                    inner: $crate::core::query_changes::QueryChangesRequest::new(since_query_state),
                }
            }

            #[must_use]
            pub fn filter(
                mut self,
                filter: impl Into<
                    $crate::core::query::Filter<<$obj as $crate::core::query::QueryObject>::Filter>,
                >,
            ) -> Self {
                self.inner.filter(filter);
                self
            }

            #[must_use]
            pub fn sort(
                mut self,
                sort: impl IntoIterator<
                    Item = $crate::core::query::Comparator<
                        <$obj as $crate::core::query::QueryObject>::Sort,
                    >,
                >,
            ) -> Self {
                self.inner.sort(sort);
                self
            }

            #[must_use]
            pub fn max_changes(mut self, max_changes: std::num::NonZeroUsize) -> Self {
                self.inner.max_changes(max_changes);
                self
            }

            #[must_use]
            pub fn up_to_id(mut self, up_to_id: impl Into<String>) -> Self {
                self.inner.up_to_id(up_to_id);
                self
            }

            #[must_use]
            pub fn calculate_total(mut self, calculate_total: bool) -> Self {
                self.inner.calculate_total(calculate_total);
                self
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::query_changes::QueryChangesRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /copy method struct that wraps `CopyRequest<O>`.
///
/// The destination `accountId` is injected by `Request::call`; the
/// source `fromAccountId` is the only constructor argument. `create()`
/// stays imperative for the same HashMap-entry reason as `set`.
#[macro_export]
macro_rules! define_copy_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(flatten)]
            inner: $crate::core::copy::CopyRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &str) {
                self.inner.account_id(account_id);
            }
        }

        impl $name {
            pub fn new(from_account_id: impl Into<String>) -> Self {
                Self {
                    inner: $crate::core::copy::CopyRequest::new(from_account_id),
                }
            }

            #[must_use]
            pub fn if_from_in_state(mut self, if_from_in_state: impl Into<String>) -> Self {
                self.inner.if_from_in_state(if_from_in_state);
                self
            }

            #[must_use]
            pub fn if_in_state(mut self, if_in_state: impl Into<String>) -> Self {
                self.inner.if_in_state(if_in_state);
                self
            }

            #[must_use]
            pub fn on_success_destroy_original(mut self, value: bool) -> Self {
                self.inner.on_success_destroy_original(value);
                self
            }

            #[must_use]
            pub fn destroy_from_if_in_state(
                mut self,
                destroy_from_if_in_state: impl Into<String>,
            ) -> Self {
                self.inner
                    .destroy_from_if_in_state(destroy_from_if_in_state);
                self
            }

            // `create` reaches through Deref/DerefMut into the inner
            // CopyRequest (which conditionally provides it when the
            // object implements `SetObjectCreatable`).
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::copy::CopyRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a Property enum with `as_str()`, `Display`, `Serialize`,
/// `Deserialize`, and `From<&str>` impls, plus an `Other(String)` catch-all.
#[macro_export]
macro_rules! define_open_property_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident => $wire:expr ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )*
            Other(String),
        }

        impl $name {
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $wire, )*
                    Self::Other(s) => s.as_str(),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct PropertyVisitor;
                impl serde::de::Visitor<'_> for PropertyVisitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("a property name")
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$name, E> {
                        Ok($name::from(v))
                    }
                }
                deserializer.deserialize_str(PropertyVisitor)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                match s {
                    $( $wire => Self::$variant, )*
                    other => Self::Other(other.to_string()),
                }
            }
        }
    };
}

/// Generates a JMAP /parse method struct with `JmapMethod` impl and
/// value-builder methods.
#[macro_export]
macro_rules! define_parse_method {
    ($name:ident, $property:ty, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub struct $name {
            #[serde(rename = "accountId")]
            account_id: String,

            #[serde(rename = "blobIds")]
            blob_ids: Vec<String>,

            #[serde(rename = "properties")]
            #[serde(skip_serializing_if = "Option::is_none")]
            properties: Option<Vec<$property>>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &str) {
                self.account_id = account_id.to_string();
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    account_id: String::new(),
                    blob_ids: Vec::new(),
                    properties: None,
                }
            }

            #[must_use]
            pub fn blob_ids<U, V>(mut self, blob_ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<String>,
            {
                self.blob_ids = blob_ids.into_iter().map(std::convert::Into::into).collect();
                self
            }

            #[must_use]
            pub fn properties(mut self, properties: impl IntoIterator<Item = $property>) -> Self {
                self.properties = Some(properties.into_iter().collect());
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}
