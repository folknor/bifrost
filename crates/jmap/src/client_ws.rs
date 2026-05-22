use std::pin::Pin;

use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt, stream::SplitSink};
use http::{HeaderValue, Uri, header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_websockets::{ClientBuilder, Connector, MaybeTlsStream, Message, WebSocketStream};

use crate::{
    DataType, PushObject,
    client::Client,
    core::{
        error::{ProblemDetails, ProblemType},
        request::Request,
        response::Response,
    },
};

const JMAP_WS_SUBPROTOCOL: &str = "jmap";

#[derive(Debug, Serialize)]
struct WebSocketRequest {
    #[serde(rename = "@type")]
    pub _type: WebSocketRequestType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    using: Vec<&'static str>,

    #[serde(rename = "methodCalls")]
    method_calls: serde_json::Value,

    #[serde(rename = "createdIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    created_ids: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketResponse {
    #[serde(rename = "methodResponses")]
    method_responses: Vec<serde_json::Value>,

    #[serde(rename = "createdIds")]
    created_ids: Option<HashMap<String, String>>,

    #[serde(rename = "sessionState")]
    session_state: String,
}

#[derive(Debug, Serialize)]
struct WebSocketPushEnable {
    #[serde(rename = "@type")]
    _type: WebSocketPushEnableType,

    #[serde(rename = "dataTypes")]
    data_types: Option<Vec<DataType>>,

    #[serde(rename = "pushState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    push_state: Option<String>,
}

#[derive(Debug, Serialize)]
struct WebSocketPushDisable {
    #[serde(rename = "@type")]
    _type: WebSocketPushDisableType,
}

#[derive(Debug, Serialize)]
enum WebSocketRequestType {
    Request,
}

#[derive(Debug, Serialize)]
enum WebSocketPushEnableType {
    WebSocketPushEnable,
}

#[derive(Debug, Serialize)]
enum WebSocketPushDisableType {
    WebSocketPushDisable,
}

#[derive(Deserialize, Debug)]
pub(crate) struct WebSocketPushObject {
    #[serde(flatten)]
    pub push: PushObject,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketError {
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,

    #[serde(rename = "type")]
    p_type: ProblemType,
    status: Option<u32>,
    title: Option<String>,
    detail: Option<String>,
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "@type")]
enum WebSocketMessage_ {
    Response(WebSocketResponse),
    StateChange(WebSocketPushObject),
    #[cfg(feature = "calendars")]
    CalendarAlert(WebSocketPushObject),
    RequestError(WebSocketError),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum WebSocketMessage {
    Response(Response),
    PushNotification(PushObject),
}

pub(crate) struct WsStream {
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    req_id: usize,
}

impl Client {
    pub async fn connect_ws(
        &self,
    ) -> crate::Result<Pin<Box<impl Stream<Item = crate::Result<WebSocketMessage>> + use<>>>> {
        let session = self.session();
        let capabilities = session
            .websocket_capabilities()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?;

        let url = capabilities.url().to_string();
        let uri: Uri = url
            .parse()
            .map_err(|e: http::uri::InvalidUri| crate::Error::InvalidUrl(e.to_string()))?;

        let authorization = self.authorization();
        let auth_value =
            HeaderValue::from_str(&authorization).map_err(crate::Error::from_invalid_header)?;

        let connector = if url.starts_with("wss") {
            let native = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(self.accept_invalid_certs)
                .build()
                .map_err(crate::Error::from_tls)?;
            Some(Connector::NativeTls(tokio_native_tls::TlsConnector::from(
                native,
            )))
        } else {
            None
        };

        let mut builder = ClientBuilder::from_uri(uri)
            .add_header(header::AUTHORIZATION, auth_value)?
            .add_header(
                header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("jmap"),
            )?;
        if let Some(ref connector) = connector {
            builder = builder.connector(connector);
        }

        let (stream, response) = builder.connect().await?;
        validate_ws_subprotocol(&response)?;
        let (tx, mut rx) = stream.split();

        *self.ws.lock().await = WsStream { tx, req_id: 0 }.into();

        Ok(Box::pin(async_stream::stream! {
            let mut saw_close = false;

            while let Some(message) = rx.next().await {
                match message {
                    Ok(message) if message.is_text() => {
                        let payload = message.into_payload();
                        match serde_json::from_slice::<WebSocketMessage_>(payload.as_ref()) {
                            Ok(message) => match message {
                                WebSocketMessage_::Response(response) => {
                                    // Deserialize the raw method responses into a Response
                                    let json = serde_json::json!({
                                        "methodResponses": response.method_responses,
                                        "createdIds": response.created_ids,
                                        "sessionState": response.session_state,
                                    });
                                    match serde_json::from_value::<Response>(json) {
                                        Ok(resp) => yield Ok(WebSocketMessage::Response(resp)),
                                        Err(e) => yield Err(crate::Error::Parse(e)),
                                    }
                                }
                                WebSocketMessage_::StateChange(push) => {
                                    yield Ok(WebSocketMessage::PushNotification(push.push))
                                }
                                #[cfg(feature = "calendars")]
                                WebSocketMessage_::CalendarAlert(push) => {
                                    yield Ok(WebSocketMessage::PushNotification(push.push))
                                }
                                WebSocketMessage_::RequestError(err) => yield Err(ProblemDetails::from(err).into()),
                            },
                            Err(err) => yield Err(err.into()),
                        }
                    }
                    Ok(message) if message.is_binary() => {
                        yield Err(crate::Error::NotParsable("binary WebSocket message".to_string()));
                    }
                    Ok(message) if message.is_close() => {
                        saw_close = true;
                    }
                    Ok(_) => (),
                    Err(err) => yield Err(err.into()),
                }
            }

            if saw_close {
                yield Err(crate::Error::WebSocketClosed);
            }
        }))
    }

    pub async fn send_ws(&self, request: Request<'_>) -> crate::Result<String> {
        let mut _ws = self.ws.lock().await;
        let ws = _ws
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?;

        // Assign request id
        let request_id = ws.req_id.to_string();
        ws.req_id += 1;

        let method_calls =
            serde_json::to_value(&request.method_calls).unwrap_or(serde_json::Value::Array(vec![]));
        ws.tx
            .send(Message::text(
                serde_json::to_string(&WebSocketRequest {
                    _type: WebSocketRequestType::Request,
                    id: request_id.clone().into(),
                    using: request.using,
                    method_calls,
                    created_ids: request.created_ids,
                })
                .unwrap_or_default(),
            ))
            .await?;

        Ok(request_id)
    }

    pub async fn enable_push_ws(
        &self,
        data_types: Option<impl IntoIterator<Item = DataType>>,
        push_state: Option<impl Into<String>>,
    ) -> crate::Result<()> {
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(
                serde_json::to_string(&WebSocketPushEnable {
                    _type: WebSocketPushEnableType::WebSocketPushEnable,
                    data_types: data_types.map(|it| it.into_iter().collect()),
                    push_state: push_state.map(std::convert::Into::into),
                })
                .unwrap_or_default(),
            ))
            .await
            .map_err(std::convert::Into::into)
    }

    pub async fn disable_push_ws(&self) -> crate::Result<()> {
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(
                serde_json::to_string(&WebSocketPushDisable {
                    _type: WebSocketPushDisableType::WebSocketPushDisable,
                })
                .unwrap_or_default(),
            ))
            .await
            .map_err(std::convert::Into::into)
    }

    pub async fn ws_ping(&self) -> crate::Result<()> {
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::ping(Bytes::new()))
            .await
            .map_err(std::convert::Into::into)
    }
}

fn validate_ws_subprotocol(response: &http::Response<()>) -> crate::Result<()> {
    let Some(protocol) = response.headers().get(header::SEC_WEBSOCKET_PROTOCOL) else {
        return Err(crate::Error::from_subprotocol(
            "server did not accept the jmap WebSocket subprotocol",
        ));
    };
    let protocol = protocol.to_str().map_err(|e| {
        crate::Error::from_subprotocol(format!("invalid Sec-WebSocket-Protocol header: {e}"))
    })?;

    if protocol.trim() == JMAP_WS_SUBPROTOCOL {
        return Ok(());
    }

    Err(crate::Error::from_subprotocol(format!(
        "server accepted WebSocket subprotocol {protocol:?}, expected {JMAP_WS_SUBPROTOCOL:?}"
    )))
}

impl From<WebSocketError> for ProblemDetails {
    fn from(problem: WebSocketError) -> Self {
        ProblemDetails::new(
            problem.p_type,
            problem.status,
            problem.title,
            problem.detail,
            problem.limit,
            problem.request_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(protocol: Option<HeaderValue>) -> http::Response<()> {
        let mut response = http::Response::new(());
        if let Some(protocol) = protocol {
            response
                .headers_mut()
                .insert(header::SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        response
    }

    #[test]
    fn accepts_jmap_subprotocol() {
        let response = response(Some(HeaderValue::from_static("jmap")));

        validate_ws_subprotocol(&response).unwrap();
    }

    #[test]
    fn rejects_missing_subprotocol() {
        let err = validate_ws_subprotocol(&response(None)).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(_))
        ));
    }

    #[test]
    fn rejects_wrong_subprotocol() {
        let response = response(Some(HeaderValue::from_static("other")));
        let err = validate_ws_subprotocol(&response).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(_))
        ));
    }
}
