use std::collections::HashMap;

use serde::Deserialize;
use serde::de;

use super::error::MethodError;
use super::method::JmapMethod;
use super::request::CallHandle;

/// A parsed JMAP response with typed method result extraction.
#[derive(Debug)]
pub(crate) struct Response {
    raw: Vec<(String, RawCallResult, String)>,
    session_state: String,
    created_ids: Option<HashMap<String, String>>,
}

/// A single method call result - either success data or a method error.
#[derive(Debug)]
enum RawCallResult {
    /// Raw JSON bytes - deserialized lazily in Response::get().
    Success(Box<serde_json::value::RawValue>),
    Error(MethodError),
}

impl Response {
    /// Extract a typed response by its call handle.
    ///
    /// Compile-time safe: the handle's type parameter ensures the response
    /// is deserialized into the correct type.
    pub(crate) fn get<M: JmapMethod>(
        &mut self,
        handle: &CallHandle<M>,
    ) -> crate::Result<M::Response> {
        let pos = self
            .raw
            .iter()
            .position(|(_, _, id)| id == &handle.call_id)
            .ok_or_else(|| crate::Error::CallNotFound(handle.call_id.clone()))?;

        let (_, result, _) = self.raw.swap_remove(pos);

        match result {
            RawCallResult::Success(raw) => {
                serde_json::from_str(raw.get()).map_err(crate::Error::from)
            }
            RawCallResult::Error(e) => Err(e.into()),
        }
    }

    pub(crate) fn session_state(&self) -> &str {
        &self.session_state
    }

    pub(crate) fn created_ids(&self) -> Option<&HashMap<String, String>> {
        self.created_ids.as_ref()
    }
}

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawEnvelope {
            #[serde(rename = "methodResponses")]
            method_responses: Vec<(String, Box<serde_json::value::RawValue>, String)>,
            #[serde(rename = "createdIds")]
            created_ids: Option<HashMap<String, String>>,
            #[serde(rename = "sessionState")]
            session_state: String,
        }

        let envelope = RawEnvelope::deserialize(deserializer)?;
        let mut raw = Vec::with_capacity(envelope.method_responses.len());

        for (method_name, data, call_id) in envelope.method_responses {
            let result = if method_name == "error" {
                let error: MethodError =
                    serde_json::from_str(data.get()).map_err(de::Error::custom)?;
                RawCallResult::Error(error)
            } else {
                RawCallResult::Success(data)
            };

            raw.push((method_name, result, call_id));
        }

        Ok(Response {
            raw,
            session_state: envelope.session_state,
            created_ids: envelope.created_ids,
        })
    }
}
