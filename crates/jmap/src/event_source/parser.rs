const MAX_EVENT_SIZE: usize = 1024 * 1024;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub(crate) enum EventType {
    Ping,
    #[default]
    State,
    #[cfg(feature = "calendars")]
    CalendarAlert,
}

#[derive(Default, Debug)]
pub(crate) struct Event {
    pub(crate) event: EventType,
    pub(crate) id: Vec<u8>,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Default)]
enum EventParserState {
    #[default]
    Init,
    Comment,
    Field,
    Value,
}

#[derive(Default, Debug)]
pub(crate) struct EventParser {
    state: EventParserState,
    field: Vec<u8>,
    value: Vec<u8>,
    bytes: Option<Vec<u8>>,
    pos: usize,
    result: Event,
}

impl EventParser {
    pub(crate) fn push_bytes(&mut self, bytes: Vec<u8>) {
        self.bytes = Some(bytes);
    }

    pub(crate) fn needs_bytes(&self) -> bool {
        self.bytes.is_none()
    }
}

impl Iterator for EventParser {
    type Item = crate::Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.bytes.as_ref()?;

        for byte in bytes.get(self.pos..)? {
            self.pos += 1;

            match self.state {
                EventParserState::Init => match byte {
                    b':' => {
                        self.state = EventParserState::Comment;
                    }
                    b'\r' | b' ' => (),
                    b'\n' => {
                        return Some(Ok(std::mem::take(&mut self.result)));
                    }
                    _ => {
                        self.state = EventParserState::Field;
                        self.field.push(*byte);
                    }
                },
                EventParserState::Comment => {
                    if *byte == b'\n' {
                        self.state = EventParserState::Init;
                    }
                }
                EventParserState::Field => match byte {
                    b'\r' => (),
                    b'\n' => {
                        self.state = EventParserState::Init;
                        self.field.clear();
                    }
                    b':' => {
                        self.state = EventParserState::Value;
                    }
                    _ => {
                        if self.field.len() >= MAX_EVENT_SIZE {
                            return Some(Err(crate::Error::Transport(
                                crate::core::transport::TransportError::new(
                                    "EventSource response is too long.",
                                ),
                            )));
                        }

                        self.field.push(*byte);
                    }
                },
                EventParserState::Value => match byte {
                    b'\r' => (),
                    b' ' if self.value.is_empty() => (),
                    b'\n' => {
                        self.state = EventParserState::Init;
                        match &self.field[..] {
                            b"id" => {
                                self.result.id.extend_from_slice(&self.value);
                            }
                            b"data" => {
                                // Per SSE spec: multiple data lines joined with \n
                                if !self.result.data.is_empty() {
                                    self.result.data.push(b'\n');
                                }
                                self.result.data.extend_from_slice(&self.value);
                            }
                            b"event" => match &self.value[..] {
                                #[cfg(feature = "calendars")]
                                b"calendarAlert" => {
                                    self.result.event = EventType::CalendarAlert;
                                }
                                b"ping" => {
                                    self.result.event = EventType::Ping;
                                }
                                _ => {
                                    self.result.event = EventType::State;
                                }
                            },
                            _ => {
                                //ignore
                            }
                        }
                        self.field.clear();
                        self.value.clear();
                    }
                    _ => {
                        if (self.field.len() + self.value.len()) >= MAX_EVENT_SIZE {
                            return Some(Err(crate::Error::Transport(
                                crate::core::transport::TransportError::new(
                                    "EventSource response is too long.",
                                ),
                            )));
                        }

                        self.value.push(*byte);
                    }
                },
            }
        }

        self.bytes = None;
        self.pos = 0;

        None
    }
}

#[cfg(test)]
mod tests {

    use super::{Event, EventType};

    #[derive(Debug, PartialEq, Eq)]
    struct EventString {
        event: EventType,
        id: String,
        data: String,
    }

    impl From<Event> for EventString {
        fn from(event: Event) -> Self {
            Self {
                event: event.event,
                id: String::from_utf8(event.id).unwrap(),
                data: String::from_utf8(event.data).unwrap(),
            }
        }
    }

    #[test]
    fn parse() {
        let mut parser = super::EventParser::default();
        let mut results = Vec::new();

        for frame in [
            Vec::from("event: state\nid:  0\ndata: test\n\n"),
            Vec::from("event: ping\nid:123\ndata: ping pa"),
            Vec::from("yload"),
            Vec::from("\n\n"),
            Vec::from(":comment\n\n"),
            Vec::from("data: YHOO\n"),
            Vec::from("data: +2\n"),
            Vec::from("data: 10\n\n"),
            Vec::from(": test stream\n"),
            Vec::from("data: first event\n"),
            Vec::from("id: 1\n\n"),
            Vec::from("data:second event\n"),
            Vec::from("id\n\n"),
            Vec::from("data:  third event\n\n"),
            Vec::from("data:hello\n\ndata: world\n\n"),
        ] {
            parser.push_bytes(frame);

            #[allow(clippy::while_let_on_iterator)]
            while let Some(event) = parser.next() {
                results.push(EventString::from(event.unwrap()));
            }
        }

        assert_eq!(
            results,
            vec![
                EventString {
                    event: EventType::State,
                    id: "0".to_string(),
                    data: "test".to_string()
                },
                EventString {
                    event: EventType::Ping,
                    id: "123".to_string(),
                    data: "ping payload".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: String::new()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: "YHOO\n+2\n10".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: "1".to_string(),
                    data: "first event".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: "second event".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: "third event".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: "hello".to_string()
                },
                EventString {
                    event: EventType::State,
                    id: String::new(),
                    data: "world".to_string()
                }
            ]
        );
    }

    // The SSE spec (WHATWG, "Interpreting an event stream") says the
    // `id` field SETS the last-event-id buffer: a second `id:` line
    // replaces the first. This parser appends. Documented, not
    // endorsed - `Last-Event-ID` resumption sends a fabricated id.
    #[test]
    fn repeated_id_fields_concatenate_instead_of_replacing() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 1\nid: 2\ndata: x\n\n"));

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.id).unwrap(), "12");
        assert_eq!(String::from_utf8(event.data).unwrap(), "x");
    }

    // `push_bytes` overwrites the buffer without resetting `pos`, and
    // nothing enforces the `needs_bytes()` precondition. A caller that
    // stops draining the iterator early - which `event_source::stream`
    // does on every yielded error, because its `break` only leaves the
    // inner `for` loop - resumes at a stale offset inside the NEW
    // buffer. Documented, not endorsed: the fix is for `push_bytes` to
    // append to (or refuse to clobber) an unconsumed buffer.
    #[test]
    fn push_bytes_over_a_partially_consumed_buffer_resumes_at_a_stale_offset() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("data: one\n\ndata: two\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.data).unwrap(), "one");
        assert!(
            !parser.needs_bytes(),
            "the buffer still holds the second event"
        );

        // 11 bytes have been consumed. Pushing a 13-byte frame resumes
        // at index 11 of it, i.e. at the two trailing newlines, so the
        // `three` payload is never seen and a bogus empty event is
        // emitted instead.
        parser.push_bytes(Vec::from("data: three\n\n"));
        let next = parser.next().expect("an event").expect("no parse error");
        assert_eq!(
            String::from_utf8(next.data).unwrap(),
            "",
            "`two` was dropped and `three` was skipped"
        );
    }

    // The size guard returns an error without clearing the overflowing
    // field or resynchronising, so the same buffer keeps producing the
    // same error. Documented, not endorsed.
    #[test]
    fn an_oversized_field_errors_repeatedly_without_resyncing() {
        let mut parser = super::EventParser::default();
        let mut frame = vec![b'x'; super::MAX_EVENT_SIZE + 4];
        frame.extend_from_slice(b": v\n\n");
        parser.push_bytes(frame);

        assert!(parser.next().expect("an item").is_err());
        assert!(
            parser.next().expect("another item").is_err(),
            "the parser never drops the oversized field, so it cannot recover"
        );
    }

    // Only `data` accumulation is unbounded: MAX_EVENT_SIZE caps a
    // single field/value pair, but `result.data` grows across every
    // `data:` line of one event with no cap at all.
    #[test]
    fn multi_line_data_accumulates_past_the_single_line_cap() {
        let mut parser = super::EventParser::default();
        let line = format!("data: {}\n", "y".repeat(1024));
        let mut frame = String::new();
        for _ in 0..64 {
            frame.push_str(&line);
        }
        frame.push('\n');
        parser.push_bytes(frame.into_bytes());

        let event = parser.next().expect("an event").expect("no parse error");
        // 64 lines of 1024 bytes plus 63 joining newlines.
        assert_eq!(event.data.len(), 64 * 1024 + 63);
    }
}
