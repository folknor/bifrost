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
    pub async fn event_source(
        &self,
        mut types: Option<impl IntoIterator<Item = DataType>>,
        close_after_state: bool,
        ping: Option<u32>,
        last_event_id: Option<&str>,
    ) -> crate::Result<impl Stream<Item = crate::Result<PushNotification>> + Unpin> {
        let mut event_source_url = String::with_capacity(self.session().event_source_url().len());

        for part in self.event_source_url() {
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
            loop {
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
                                    Err(err) => { yield Err(err.into()); break; }
                                }
                            }
                            #[cfg(feature = "calendars")]
                            EventType::CalendarAlert => {
                                match serde_json::from_slice::<CalendarAlert>(&event.data) {
                                    Ok(alert) => { yield Ok(PushNotification::CalendarAlert(alert)); }
                                    Err(err) => { yield Err(err.into()); break; }
                                }
                            }
                            EventType::Ping => {}
                        },
                        Err(err) => { yield Err(err); break; }
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
