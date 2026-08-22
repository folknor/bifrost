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
    pub(crate) _type: WebSocketRequestType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,

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

#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketError {
    #[serde(rename = "requestId")]
    pub(crate) request_id: Option<String>,

    #[serde(rename = "type")]
    p_type: ProblemType,
    status: Option<u32>,
    title: Option<String>,
    detail: Option<String>,
    limit: Option<String>,
}

// RFC 8887 frames carry exactly one `@type` discriminator. This
// enum's tag consumes it, so the payload variants must inline the
// post-tag fields directly. A nested `#[serde(tag = "@type")]` type
// (e.g. `PushObject`) would look for a second `@type` that the wire
// never sends and fail with `protocol.parse-failed`. The match arm
// below rebuilds a `PushObject` for the downstream stream output.
#[derive(Debug, Deserialize)]
#[serde(tag = "@type")]
enum WebSocketMessage_ {
    Response(WebSocketResponse),
    StateChange {
        changed: HashMap<String, HashMap<DataType, String>>,
    },
    #[cfg(feature = "calendars")]
    CalendarAlert(crate::CalendarAlert),
    RequestError(WebSocketError),
}

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum WebSocketMessage {
    Response(Response),
    PushNotification(PushObject),
    /// A control-frame pong. Surfaced rather than swallowed because it is
    /// the only evidence the push reader can get that a silent connection
    /// is still alive: JMAP defines no application-level keepalive, so a
    /// half-open socket and a quiet mailbox look identical on this stream
    /// until something answers a ping.
    Pong,
}

pub(crate) struct WsStream {
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    req_id: usize,
}

impl Client {
    pub(crate) async fn connect_ws(
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

        let authorization = self.authorization().await?;
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
            .add_header(header::AUTHORIZATION, auth_value)
            .map_err(crate::Error::WebSocketHandshake)?
            .add_header(
                header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("jmap"),
            )
            .map_err(crate::Error::WebSocketHandshake)?;
        if let Some(ref connector) = connector {
            builder = builder.connector(connector);
        }

        // Pre-handshake error: no bytes from the JMAP request itself
        // have crossed the side-effect boundary. Classification is
        // `Transport(Network) + Attempt(Unsent)`, not `Protocol(_)`.
        let (stream, response) = builder
            .connect()
            .await
            .map_err(crate::Error::WebSocketHandshake)?;
        validate_ws_subprotocol(&response)?;
        let (tx, rx) = stream.split();

        *self.ws.lock().await = WsStream { tx, req_id: 0 }.into();

        Ok(Box::pin(frame_stream(rx)))
    }

    pub(crate) async fn send_ws(&self, request: Request<'_>) -> crate::Result<String> {
        let mut _ws = self.ws.lock().await;
        let ws = _ws
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?;

        // Assign request id
        let request_id = ws.req_id.to_string();
        ws.req_id += 1;

        let method_calls =
            serde_json::to_value(&request.method_calls).map_err(crate::Error::RequestEncode)?;
        let frame = serde_json::to_string(&WebSocketRequest {
            _type: WebSocketRequestType::Request,
            id: request_id.clone().into(),
            using: request.using,
            method_calls,
            created_ids: request.created_ids,
        })
        .map_err(crate::Error::RequestEncode)?;
        ws.tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketRuntime)?;

        Ok(request_id)
    }

    pub(crate) async fn enable_push_ws(
        &self,
        data_types: Option<impl IntoIterator<Item = DataType>>,
        push_state: Option<impl Into<String>>,
    ) -> crate::Result<()> {
        let frame = serde_json::to_string(&WebSocketPushEnable {
            _type: WebSocketPushEnableType::WebSocketPushEnable,
            data_types: data_types.map(|it| it.into_iter().collect()),
            push_state: push_state.map(std::convert::Into::into),
        })
        .map_err(crate::Error::RequestEncode)?;
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketRuntime)
    }

    pub(crate) async fn disable_push_ws(&self) -> crate::Result<()> {
        let frame = serde_json::to_string(&WebSocketPushDisable {
            _type: WebSocketPushDisableType::WebSocketPushDisable,
        })
        .map_err(crate::Error::RequestEncode)?;
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::text(frame))
            .await
            .map_err(crate::Error::WebSocketRuntime)
    }

    pub(crate) async fn ws_ping(&self) -> crate::Result<()> {
        self.ws
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| crate::Error::WebSocketNotConnected)?
            .tx
            .send(Message::ping(Bytes::new()))
            .await
            .map_err(crate::Error::WebSocketRuntime)
    }
}

/// Frame-level decoding of an established JMAP WebSocket read half.
///
/// Split out from `connect_ws` so the close / error / binary arms can be
/// driven from an in-memory stream of `Message`s without a socket: the
/// transport half is the only part of `connect_ws` that needs a real
/// connection.
fn frame_stream<S>(mut rx: S) -> impl Stream<Item = crate::Result<WebSocketMessage>> + use<S>
where
    S: Stream<Item = Result<Message, tokio_websockets::Error>> + Unpin,
{
    async_stream::stream! {
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
                                    Err(e) => yield Err(crate::Error::ResponseDecode(e)),
                                }
                            }
                            WebSocketMessage_::StateChange { changed } => {
                                yield Ok(WebSocketMessage::PushNotification(PushObject::StateChange { changed }))
                            }
                            #[cfg(feature = "calendars")]
                            WebSocketMessage_::CalendarAlert(alert) => {
                                yield Ok(WebSocketMessage::PushNotification(PushObject::CalendarAlert(alert)))
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
                Ok(message) if message.is_pong() => {
                    yield Ok(WebSocketMessage::Pong);
                }
                Ok(_) => (),
                // Post-handshake runtime drop. Classification is
                // `Protocol(PartialResponse) + Attempt(Acknowledged)`.
                Err(err) => yield Err(crate::Error::WebSocketRuntime(err)),
            }
        }

        if saw_close {
            yield Err(crate::Error::WebSocketClosed);
        }
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

    // A real RFC 8887 StateChange frame carries exactly one `@type`.
    // The outer enum tag consumes it, so the variant must inline
    // `changed` directly; a nested `#[serde(tag = "@type")]` payload
    // would demand a second `@type` the wire never sends.
    #[test]
    fn deserializes_single_type_state_change_frame() {
        let frame = r#"{"@type":"StateChange","changed":{"u1138":{"Mailbox":"f9a8d3"}}}"#;

        let message: WebSocketMessage_ = serde_json::from_str(frame).unwrap();

        let WebSocketMessage_::StateChange { changed } = message else {
            panic!("expected StateChange variant, got {message:?}");
        };

        let by_type = changed.get("u1138").expect("account entry present");
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type.values().next().map(String::as_str), Some("f9a8d3"));
    }

    /// Stub read half of a JMAP WebSocket. Same shape as the stub
    /// `HttpTransport` / `SseTransport` doubles the blob and EventSource
    /// tests use: a canned transcript replayed in order, no socket.
    struct StubWsTransport {
        frames: Vec<Result<Message, tokio_websockets::Error>>,
    }

    impl StubWsTransport {
        fn new(frames: Vec<Result<Message, tokio_websockets::Error>>) -> Self {
            Self { frames }
        }

        fn open_ws(self) -> impl Stream<Item = Result<Message, tokio_websockets::Error>> + Unpin {
            futures::stream::iter(self.frames)
        }
    }

    fn io_error() -> tokio_websockets::Error {
        tokio_websockets::Error::Io(std::io::Error::other("peer reset"))
    }

    async fn decode(
        frames: Vec<Result<Message, tokio_websockets::Error>>,
    ) -> Vec<crate::Result<WebSocketMessage>> {
        frame_stream(StubWsTransport::new(frames).open_ws())
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn binary_frame_is_rejected_as_not_parsable() {
        let out = decode(vec![Ok(Message::binary(Bytes::from_static(b"\x00\x01")))]).await;

        assert_eq!(out.len(), 1);
        let Err(crate::Error::NotParsable(what)) = &out[0] else {
            panic!("expected NotParsable, got {:?}", out[0]);
        };
        assert_eq!(what, "binary WebSocket message");
    }

    #[tokio::test]
    async fn close_frame_yields_closed_only_after_the_stream_ends() {
        let out = decode(vec![
            Ok(Message::close(None, "")),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        // The close arm only records; frames queued behind it are still
        // decoded, and `WebSocketClosed` is emitted last.
        assert_eq!(out.len(), 2);
        assert!(matches!(
            out[0],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
        assert!(matches!(out[1], Err(crate::Error::WebSocketClosed)));
    }

    #[tokio::test]
    async fn stream_end_without_close_frame_is_silent() {
        let out = decode(vec![Ok(Message::ping(Bytes::new()))]).await;

        // Ping / pong fall through the `Ok(_)` arm, and an EOF that was
        // not preceded by a close frame yields nothing at all.
        assert!(out.is_empty(), "expected no items, got {out:?}");
    }

    #[tokio::test]
    async fn runtime_error_is_yielded_and_the_stream_keeps_reading() {
        let out = decode(vec![
            Err(io_error()),
            Ok(Message::text(
                r#"{"@type":"StateChange","changed":{"u1":{"Mailbox":"s1"}}}"#.to_string(),
            )),
        ])
        .await;

        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Err(crate::Error::WebSocketRuntime(_))));
        assert!(matches!(
            out[1],
            Ok(WebSocketMessage::PushNotification(
                PushObject::StateChange { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn malformed_text_frame_is_a_response_decode_error() {
        let out = decode(vec![Ok(Message::text("not json".to_string()))]).await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::ResponseDecode(_))));
    }

    #[tokio::test]
    async fn request_error_frame_becomes_problem_details() {
        let frame = r#"{"@type":"RequestError","requestId":"7","type":"urn:ietf:params:jmap:error:limit","status":400,"limit":"maxSizeRequest"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Err(crate::Error::Problem { details, transport }) = &out[0] else {
            panic!("expected Problem, got {:?}", out[0]);
        };
        assert!(transport.is_none());
        assert_eq!(details.status(), Some(400));
        assert_eq!(details.limit(), Some("maxSizeRequest"));
        assert_eq!(details.request_id(), Some("7"));
    }

    #[tokio::test]
    async fn response_frame_is_rebuilt_into_a_response() {
        let frame = r#"{"@type":"Response","methodResponses":[["Core/echo",{"hello":true},"c0"]],"createdIds":{"k":"v"},"sessionState":"s-1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Ok(WebSocketMessage::Response(response)) = &out[0] else {
            panic!("expected Response, got {:?}", out[0]);
        };
        assert_eq!(response.session_state(), "s-1");
        assert_eq!(
            response.created_ids().and_then(|ids| ids.get("k")),
            Some(&"v".to_string())
        );
    }

    #[tokio::test]
    async fn response_frame_with_malformed_method_responses_decodes_to_an_error() {
        // The outer frame parses (methodResponses is a JSON array), but
        // the rebuilt envelope is not a valid `Response`: this pins the
        // inner `from_value` failure arm distinctly from the outer one.
        let frame = r#"{"@type":"Response","methodResponses":[[1,2]],"sessionState":"s-1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Err(crate::Error::ResponseDecode(_))));
    }

    #[cfg(feature = "calendars")]
    #[tokio::test]
    async fn calendar_alert_frame_becomes_a_push_notification() {
        let frame = r#"{"@type":"CalendarAlert","accountId":"a1","calendarEventId":"e1","uid":"u1","alertId":"al1"}"#;

        let out = decode(vec![Ok(Message::text(frame.to_string()))]).await;

        assert_eq!(out.len(), 1);
        let Ok(WebSocketMessage::PushNotification(PushObject::CalendarAlert(alert))) = &out[0]
        else {
            panic!("expected CalendarAlert, got {:?}", out[0]);
        };
        assert_eq!(alert.alert_id, "al1");
        assert_eq!(alert.recurrence_id, None);
    }
}
