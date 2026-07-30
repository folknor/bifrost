#[cfg(feature = "calendars")]
use crate::event_source::CalendarAlert;
use crate::{
    DataType, PushObject,
    client::Client,
    core::{
        session::URLPart,
        transport::{HttpTransport, SseTransport},
    },
    event_source::{
        Changes, PushNotification,
        parser::{EventParser, EventType},
    },
};
use futures::{Stream, StreamExt};

impl<T: HttpTransport + SseTransport> Client<T> {
    pub(crate) async fn event_source(
        &self,
        mut types: Option<impl IntoIterator<Item = DataType>>,
        close_after_state: bool,
        ping: Option<u32>,
        last_event_id: Option<&str>,
    ) -> crate::Result<impl Stream<Item = crate::Result<PushNotification>> + Unpin> {
        let state = self.session_state();
        let mut event_source_url = String::with_capacity(state.session().event_source_url().len());

        for part in state.event_source_url() {
            match part {
                URLPart::Value(value) => {
                    event_source_url.push_str(value);
                }
                URLPart::Parameter(param) => match param {
                    super::URLParameter::Types => {
                        if let Some(types) = types.take() {
                            event_source_url.push_str(
                                &types
                                    .into_iter()
                                    .map(|t| t.to_string())
                                    .collect::<Vec<_>>()
                                    .join(","),
                            );
                        } else {
                            event_source_url.push('*');
                        }
                    }
                    super::URLParameter::CloseAfter => {
                        event_source_url.push_str(if close_after_state { "state" } else { "no" });
                    }
                    super::URLParameter::Ping => {
                        if let Some(ping) = ping {
                            event_source_url.push_str(&ping.to_string());
                        } else {
                            event_source_url.push('0');
                        }
                    }
                },
            }
        }

        let mut stream = self
            .transport()
            .open_sse(&event_source_url, last_event_id)
            .await
            .map_err(crate::Error::from)?;

        let mut parser = EventParser::default();

        Ok(Box::pin(async_stream::stream! {
            'events: loop {
                for event_result in parser.by_ref() {
                    match event_result {
                        Ok(event) => match event.event {
                            EventType::State => {
                                match serde_json::from_slice::<PushObject>(&event.data) {
                                    Ok(PushObject::StateChange { changed }) => {
                                        yield Ok(PushNotification::StateChange(Changes::new(
                                            if event.id.is_empty() { None } else { Some(String::from_utf8_lossy(&event.id).into_owned()) },
                                            changed,
                                        )));
                                    }
                                    Ok(_) => {}
                                    Err(err) => { yield Err(err.into()); break 'events; }
                                }
                            }
                            #[cfg(feature = "calendars")]
                            EventType::CalendarAlert => {
                                match serde_json::from_slice::<CalendarAlert>(&event.data) {
                                    Ok(alert) => { yield Ok(PushNotification::CalendarAlert(alert)); }
                                    Err(err) => { yield Err(err.into()); break 'events; }
                                }
                            }
                            EventType::Ping => {}
                        },
                        Err(err) => { yield Err(err); break 'events; }
                    }
                    continue;
                }
                if let Some(result) = stream.next().await {
                    match result {
                        Ok(bytes) => {
                            parser.push_bytes(bytes);
                            continue;
                        }
                        Err(err) => {
                            yield Err(crate::Error::from(err));
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use serde_json::json;

    use crate::core::{
        session::Session,
        transport::{HttpTransport, TransportError},
    };

    struct MalformedEventTransport;

    impl HttpTransport for MalformedEventTransport {
        async fn api_request(&self, _: &str, _: Vec<u8>) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not make API requests")
        }

        async fn upload(
            &self,
            _: &str,
            _: Vec<u8>,
            _: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not upload")
        }

        async fn download(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not download")
        }

        async fn get_session(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test receives a pre-fetched session")
        }
    }

    impl SseTransport for MalformedEventTransport {
        type ByteStream = stream::Iter<std::vec::IntoIter<Result<Vec<u8>, TransportError>>>;

        async fn open_sse(
            &self,
            _: &str,
            _: Option<&str>,
        ) -> Result<Self::ByteStream, TransportError> {
            Ok(stream::iter(vec![
                Ok(Vec::from("data: not JSON\n\n")),
                Ok(Vec::from(
                    "data: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
                )),
            ]))
        }
    }

    struct HeartbeatTransport;

    impl HttpTransport for HeartbeatTransport {
        async fn api_request(&self, _: &str, _: Vec<u8>) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not make API requests")
        }

        async fn upload(
            &self,
            _: &str,
            _: Vec<u8>,
            _: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not upload")
        }

        async fn download(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not download")
        }

        async fn get_session(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test receives a pre-fetched session")
        }
    }

    impl SseTransport for HeartbeatTransport {
        type ByteStream = stream::Iter<std::vec::IntoIter<Result<Vec<u8>, TransportError>>>;

        async fn open_sse(
            &self,
            _: &str,
            _: Option<&str>,
        ) -> Result<Self::ByteStream, TransportError> {
            Ok(stream::iter(vec![
                Ok(Vec::from(": keepalive\n\n")),
                Ok(Vec::from(
                    "id: c1\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
                )),
                Ok(Vec::from(": keepalive\n\n")),
                Ok(Vec::from(
                    "data: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
                )),
            ]))
        }
    }

    fn session() -> Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "u@example.org",
            "apiUrl": "https://example.org/jmap/",
            "downloadUrl": "https://example.org/dl/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.org/ul/{accountId}/",
            "eventSourceUrl": "https://example.org/es/?types={types}&closeafter={closeafter}&ping={ping}",
            "state": "s0"
        }))
        .expect("test session decodes")
    }

    #[tokio::test]
    async fn malformed_event_terminates_the_stream_without_reading_on() {
        let client = Client::with_transport(
            MalformedEventTransport,
            session(),
            "https://example.org/.well-known/jmap",
        )
        .expect("client accepts the test session");
        let mut events = client
            .event_source(None::<Vec<DataType>>, false, None, None)
            .await
            .expect("EventSource opens");

        assert!(events.next().await.expect("a parser error").is_err());
        assert!(events.next().await.is_none());
    }

    // Comment lines are how servers keep a long-lived EventSource warm.
    // They must not surface as events, and they must not tear the stream
    // down; the resume token from the last `id` field carries across the
    // events that follow it.
    #[tokio::test]
    async fn comment_heartbeats_do_not_disturb_the_stream() {
        let client = Client::with_transport(
            HeartbeatTransport,
            session(),
            "https://example.org/.well-known/jmap",
        )
        .expect("client accepts the test session");
        let mut events = client
            .event_source(None::<Vec<DataType>>, false, None, None)
            .await
            .expect("EventSource opens");

        for expected in ["c1", "c1"] {
            let notification = events
                .next()
                .await
                .expect("a state change")
                .expect("no decode error");
            let PushNotification::StateChange(changes) = notification else {
                panic!("expected a state change");
            };
            assert_eq!(changes.id(), Some(expected));
        }
        assert!(events.next().await.is_none());
    }
}
