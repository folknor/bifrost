use std::marker::PhantomData;

use serde::ser::{SerializeStruct, SerializeTuple};
use serde::{Serialize, Serializer};

use crate::client::Client;
use crate::core::transport::HttpTransport;

use super::capability::Capability;
use super::method::JmapMethod;
use super::response::Response;

/// A typed handle to a method call in a request batch.
///
/// The type parameter `M` ties this handle to the method that produced it,
/// ensuring compile-time safety when extracting the response.
pub struct CallHandle<M: JmapMethod> {
    pub(crate) call_id: String,
    pub(crate) method_name: &'static str,
    pub(crate) _phantom: PhantomData<M>,
}

impl<M: JmapMethod> CallHandle<M> {
    /// Create a result reference pointing to a path in this call's response.
    ///
    /// Example: `handle.result_reference("/ids")` references the `ids` array
    /// from a query response.
    pub fn result_reference(&self, path: impl Into<String>) -> ResultReference {
        ResultReference {
            result_of: self.call_id.clone(),
            name: self.method_name,
            path: path.into(),
        }
    }
}

/// A JMAP result reference (RFC 8620 §3.7).
#[derive(Debug, Clone, Serialize)]
pub struct ResultReference {
    #[serde(rename = "resultOf")]
    pub(crate) result_of: String,
    pub(crate) name: &'static str,
    pub(crate) path: String,
}

/// A type-erased method call stored in the request batch.
pub(crate) struct RawMethodCall {
    pub(crate) name: &'static str,
    pub(crate) arguments: Box<serde_json::value::RawValue>,
    pub(crate) call_id: String,
}

impl Serialize for RawMethodCall {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(3)?;
        tuple.serialize_element(self.name)?;
        tuple.serialize_element(&self.arguments)?;
        tuple.serialize_element(&self.call_id)?;
        tuple.end()
    }
}

/// A JMAP request batch.
pub struct Request<'x, T: HttpTransport = crate::transport_reqwest::ReqwestTransport> {
    client: &'x Client<T>,
    account_id: String,
    pub(crate) using: Vec<&'static str>,
    pub(crate) method_calls: Vec<RawMethodCall>,
    pub(crate) created_ids: Option<std::collections::HashMap<String, String>>,
}

impl<T: HttpTransport> Serialize for Request<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let field_count = if self.created_ids.is_some() { 3 } else { 2 };
        let mut s = serializer.serialize_struct("Request", field_count)?;
        s.serialize_field("using", &self.using)?;
        s.serialize_field("methodCalls", &self.method_calls)?;
        if let Some(ref ids) = self.created_ids {
            s.serialize_field("createdIds", ids)?;
        }
        s.end()
    }
}

impl<'x, T: HttpTransport> Request<'x, T> {
    pub fn new(client: &'x Client<T>) -> Self {
        Request {
            using: vec!["urn:ietf:params:jmap:core"],
            method_calls: Vec::new(),
            created_ids: None,
            account_id: client.default_account_id().to_string(),
            client,
        }
    }

    pub fn account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = account_id.into();
        self
    }

    /// The default account ID for this request.
    pub fn default_account_id(&self) -> &str {
        &self.account_id
    }

    /// Add a method call to the batch. Returns a typed handle for
    /// extracting the response later.
    pub fn call<M: JmapMethod>(&mut self, method: M) -> Result<CallHandle<M>, crate::Error> {
        let call_id = format!("s{}", self.method_calls.len());

        // Auto-add capability
        let uri = M::Cap::URI;
        if !self.using.contains(&uri) {
            self.using.push(uri);
        }

        // Serialize method arguments once as raw JSON
        let arguments = serde_json::value::to_raw_value(&method)?;

        self.method_calls.push(RawMethodCall {
            name: M::NAME,
            arguments,
            call_id: call_id.clone(),
        });

        Ok(CallHandle {
            call_id,
            method_name: M::NAME,
            _phantom: PhantomData,
        })
    }

    /// Add a capability URI to the `using` array.
    pub fn add_capability<C: Capability>(&mut self) {
        let uri = C::URI;
        if !self.using.contains(&uri) {
            self.using.push(uri);
        }
    }

    /// Send the request and get the full response.
    pub async fn send(self) -> crate::Result<Response> {
        self.client.send_request(&self).await
    }

    /// Send the request and extract the response for the given handle.
    ///
    /// Validates that the response contains a matching call ID and handles
    /// method errors. Equivalent to `send().await?.get(&handle)?`.
    pub async fn send_single<M: JmapMethod>(
        self,
        handle: &CallHandle<M>,
    ) -> crate::Result<M::Response> {
        let mut response = self.send().await?;
        response.get(handle)
    }
}

#[cfg(feature = "websockets")]
impl Request<'_, crate::transport_reqwest::ReqwestTransport> {
    pub async fn send_ws(self) -> crate::Result<String> {
        self.client.send_ws(self).await
    }
}
