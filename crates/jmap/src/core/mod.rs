use std::fmt::Display;
use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub mod capability;
pub mod changes;
pub mod copy;
pub mod error;
pub mod field;
pub mod get;
pub mod id;
pub mod method;
pub mod parse;
pub mod query;
pub mod query_changes;
pub mod request;
pub mod response;
pub mod session;
pub mod set;
pub mod transport;

#[cfg(test)]
mod tests;

/// The base trait for a JMAP object's server-returned (Get) shape.
pub trait Object: Sized {
    type Property: Display + Serialize + DeserializeOwned;
    /// The strongly-typed ID for this object (`EmailId`, `MailboxId`,
    /// etc.). Used as the typed key/value type in generic core
    /// `SetRequest`/`SetResponse`/`GetRequest`/`GetResponse`/`Changes`/
    /// `Query`. Per-object impls set this to their `Id<marker::Foo>`
    /// typedef.
    type Id: Clone + Hash + Eq + Display + Serialize + DeserializeOwned + From<String> + Send;
    fn requires_account_id() -> bool;
}

/// The trait implemented by a Create-shape input struct (e.g.
/// `MailboxCreate`, `EmailCreate`).
pub trait SetCreate: Sized {
    fn create_id(&self) -> Option<String>;
    fn new(create_id: Option<usize>) -> Self;
}

/// Generates the trio of JSON-map-backed JMAP types: Get-shape
/// (deserializable, holds whatever the server sent), Create-shape
/// (settable, no `_id`, carries `_create_id`), Patch-shape (settable,
/// allows dotted-path keys for nested patches).
///
/// Each type wraps a `serde_json::Map` so vendor extension properties
/// survive round-trip. Used for CalendarEvent and ContactCard.
#[macro_export]
macro_rules! json_object_struct {
    ($name:ident, $create:ident, $patch:ident, $expecting:expr) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            /// The raw properties map. Every key/value from the server
            /// is preserved, including vendor extension properties.
            pub properties: serde_json::Map<String, serde_json::Value>,
        }

        #[derive(Debug, Clone)]
        pub struct $create {
            pub(super) _create_id: Option<usize>,
            pub properties: serde_json::Map<String, serde_json::Value>,
        }

        #[derive(Debug, Clone, Default)]
        pub struct $patch {
            /// Dotted-path patch keys (e.g. `participants/p1/name`) are
            /// permitted by JMAP /set update semantics.
            pub properties: serde_json::Map<String, serde_json::Value>,
        }

        $crate::__json_object_serde!($name, $expecting, with_deserialize);
        $crate::__json_object_serde!($create, $expecting, no_deserialize);
        $crate::__json_object_serde!($patch, $expecting, no_deserialize);

        impl $crate::core::SetCreate for $create {
            fn create_id(&self) -> Option<String> {
                self._create_id.map(|id| format!("c{id}"))
            }
            fn new(_create_id: Option<usize>) -> Self {
                Self {
                    _create_id,
                    properties: serde_json::Map::new(),
                }
            }
        }

        impl $create {
            pub fn set_property(
                &mut self,
                name: impl Into<String>,
                value: serde_json::Value,
            ) -> &mut Self {
                self.properties.insert(name.into(), value);
                self
            }
            pub fn properties_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
                &mut self.properties
            }
        }

        impl $patch {
            pub fn set_property(
                &mut self,
                name: impl Into<String>,
                value: serde_json::Value,
            ) -> &mut Self {
                self.properties.insert(name.into(), value);
                self
            }
            pub fn properties_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
                &mut self.properties
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __json_object_serde {
    ($name:ident, $expecting:expr, with_deserialize) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(self.properties.len()))?;
                for (k, v) in &self.properties {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> serde::de::Visitor<'de> for V {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str($expecting)
                    }
                    fn visit_map<M: serde::de::MapAccess<'de>>(
                        self,
                        mut map: M,
                    ) -> Result<Self::Value, M::Error> {
                        let mut properties = serde_json::Map::new();
                        while let Some((key, value)) =
                            map.next_entry::<String, serde_json::Value>()?
                        {
                            properties.insert(key, value);
                        }
                        Ok($name { properties })
                    }
                }
                deserializer.deserialize_map(V)
            }
        }
    };
    ($name:ident, $expecting:expr, no_deserialize) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(self.properties.len()))?;
                for (k, v) in &self.properties {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    };
}
