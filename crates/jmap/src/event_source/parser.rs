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
    Discard,
}

#[derive(Default, Debug)]
pub(crate) struct EventParser {
    state: EventParserState,
    field: Vec<u8>,
    value: Vec<u8>,
    bytes: Option<Vec<u8>>,
    pos: usize,
    result: Event,
    discard_at_line_start: bool,
    // Running length of the comment line being skipped; see the Comment arm.
    comment_len: usize,
    // WHATWG "interpreting an event stream": the last event ID buffer is
    // NOT reset between events. It survives until another `id` field
    // changes it, and every dispatched event carries the current value.
    last_event_id: Vec<u8>,
    // The spec dispatches nothing when the data buffer is empty, which is
    // what makes a comment-only keepalive block (":\n\n") a no-op. An
    // explicit flag is used rather than `result.data.is_empty()` so that a
    // bare `data:\n\n` still dispatches an event with an empty payload.
    data_seen: bool,
    // WHATWG: lines end with CRLF, LF, or CR. A CR is treated as a line
    // terminator immediately; this flag swallows the LF of a CRLF pair,
    // including one split across two `push_bytes` frames.
    skip_lf: bool,
    // WHATWG "process the field": at most ONE space after the colon is
    // stripped from a value; `data:  x` carries the value ` x`.
    strip_value_space: bool,
}

impl EventParser {
    pub(crate) fn push_bytes(&mut self, mut bytes: Vec<u8>) {
        if let Some(mut buffered) = self.bytes.take() {
            let offset = self.pos.min(buffered.len());
            let mut remaining = buffered.split_off(offset);
            remaining.append(&mut bytes);
            bytes = remaining;
        }

        self.bytes = (!bytes.is_empty()).then_some(bytes);
        self.pos = 0;
    }

    pub(crate) fn needs_bytes(&self) -> bool {
        self.bytes.is_none()
    }

    /// Applies the buffered `field`/`value` pair, per the WHATWG
    /// "processing the field" rules. Returns `false` when the accumulated
    /// data would exceed [`MAX_EVENT_SIZE`], in which case the caller must
    /// enter the discard state.
    fn commit_field(&mut self) -> bool {
        match &self.field[..] {
            b"id" => {
                // A colonless `id` line carries an empty value, which the
                // spec treats as clearing the buffer. An id containing
                // U+0000 NULL is ignored outright (WHATWG); it is the
                // resume token a reconnect echoes into a `Last-Event-ID`
                // header, where a NUL is never a legal byte.
                if !self.value.contains(&0) {
                    self.last_event_id = std::mem::take(&mut self.value);
                }
            }
            b"data" => {
                // Per SSE spec: multiple data lines joined with \n
                let separator_len = usize::from(self.data_seen);
                if self.result.data.len() + separator_len + self.value.len() > MAX_EVENT_SIZE {
                    return false;
                }
                if separator_len != 0 {
                    self.result.data.push(b'\n');
                }
                self.result.data.extend_from_slice(&self.value);
                self.data_seen = true;
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
        true
    }

    /// Drops every partial buffer except the persistent last event ID and
    /// enters the resynchronising discard state.
    fn discard(&mut self, at_line_start: bool) {
        self.state = EventParserState::Discard;
        self.field.clear();
        self.value.clear();
        self.result = Event::default();
        self.data_seen = false;
        self.strip_value_space = false;
        self.discard_at_line_start = at_line_start;
    }

    fn too_long_error() -> crate::Error {
        crate::Error::Transport(crate::core::transport::TransportError::new(
            "EventSource response is too long.",
        ))
    }
}

impl Iterator for EventParser {
    type Item = crate::Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .bytes
            .as_ref()
            .is_some_and(|bytes| self.pos >= bytes.len())
        {
            self.bytes = None;
            self.pos = 0;
            return None;
        }
        self.bytes.as_ref()?;

        while let Some(byte) = self
            .bytes
            .as_ref()
            .and_then(|bytes| bytes.get(self.pos))
            .copied()
        {
            self.pos += 1;

            // Normalise the three WHATWG line terminators (CRLF, LF, CR)
            // to a single `\n` before the state machine sees the byte.
            if std::mem::take(&mut self.skip_lf) && byte == b'\n' {
                continue;
            }
            let byte = if byte == b'\r' {
                self.skip_lf = true;
                b'\n'
            } else {
                byte
            };

            match self.state {
                EventParserState::Init => match byte {
                    b':' => {
                        self.state = EventParserState::Comment;
                        self.comment_len = 0;
                    }
                    b'\n' => {
                        // A block that carried no `data` field dispatches
                        // nothing (comment-only keepalives land here); the
                        // event type buffer is still reset.
                        if self.data_seen {
                            self.data_seen = false;
                            let mut event = std::mem::take(&mut self.result);
                            event.id.clone_from(&self.last_event_id);
                            return Some(Ok(event));
                        }
                        self.result = Event::default();
                    }
                    _ => {
                        self.state = EventParserState::Field;
                        self.field.push(byte);
                    }
                },
                EventParserState::Comment => {
                    if byte == b'\n' {
                        self.state = EventParserState::Init;
                    } else {
                        // A comment is skipped, not buffered, so the cap is
                        // a counter: without it a pathological server could
                        // stream one unterminated comment forever.
                        self.comment_len += 1;
                        if self.comment_len > MAX_EVENT_SIZE {
                            self.discard(false);
                            return Some(Err(Self::too_long_error()));
                        }
                    }
                }
                EventParserState::Field => match byte {
                    b'\n' => {
                        self.state = EventParserState::Init;
                        // A field name with no colon is a field with an
                        // empty value, not a line to throw away.
                        if !self.commit_field() {
                            self.discard(true);
                            return Some(Err(Self::too_long_error()));
                        }
                    }
                    b':' => {
                        self.state = EventParserState::Value;
                        self.strip_value_space = true;
                    }
                    _ => {
                        if self.field.len() >= MAX_EVENT_SIZE {
                            self.discard(false);
                            return Some(Err(Self::too_long_error()));
                        }

                        self.field.push(byte);
                    }
                },
                EventParserState::Value => {
                    let strip_one_space = std::mem::take(&mut self.strip_value_space);
                    match byte {
                        b' ' if strip_one_space => (),
                        b'\n' => {
                            self.state = EventParserState::Init;
                            if !self.commit_field() {
                                self.discard(true);
                                return Some(Err(Self::too_long_error()));
                            }
                        }
                        _ => {
                            if (self.field.len() + self.value.len()) >= MAX_EVENT_SIZE {
                                self.discard(false);
                                return Some(Err(Self::too_long_error()));
                            }

                            self.value.push(byte);
                        }
                    }
                }
                EventParserState::Discard => {
                    if byte == b'\n' {
                        if self.discard_at_line_start {
                            self.state = EventParserState::Init;
                            self.discard_at_line_start = false;
                        } else {
                            self.discard_at_line_start = true;
                        }
                    } else {
                        self.discard_at_line_start = false;
                    }
                }
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
                    // `id:  0` strips exactly one space; the second is
                    // part of the value (WHATWG "process the field").
                    id: " 0".to_string(),
                    data: "test".to_string()
                },
                EventString {
                    event: EventType::Ping,
                    id: "123".to_string(),
                    data: "ping payload".to_string()
                },
                // `:comment\n\n` carries no data field, so it dispatches
                // nothing at all - it is a keepalive.
                EventString {
                    event: EventType::State,
                    // The last event ID buffer persists across events.
                    id: "123".to_string(),
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
                    data: " third event".to_string()
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

    // Servers send comment-only blocks as keepalive heartbeats. Per the
    // WHATWG event-stream rules a block whose data buffer is empty
    // dispatches nothing; emitting an empty event here used to make the
    // consumer in `stream.rs` fail an empty JSON parse and hang up on a
    // live connection.
    #[test]
    fn comment_only_blocks_dispatch_nothing() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from(
            ": keepalive\n\n:\n\nevent: state\n\ndata: real\n\n",
        ));

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.data).unwrap(), "real");
        assert!(parser.next().is_none(), "only one event was dispatched");
    }

    // A `data` line with an empty value is still a data field, so it
    // dispatches an event with an empty payload.
    #[test]
    fn an_empty_data_field_still_dispatches() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("data\n\n"));

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.data).unwrap(), "");
    }

    #[test]
    fn the_last_event_id_buffer_persists_across_events() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 7\ndata: a\n\ndata: b\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.id).unwrap(), "7");
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(
            String::from_utf8(second.id).unwrap(),
            "7",
            "an event without an `id` field keeps the previous resume token"
        );
    }

    #[test]
    fn a_colonless_id_line_clears_the_buffer() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 7\ndata: a\n\nid\ndata: b\n\ndata: c\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.id).unwrap(), "7");
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(second.id).unwrap(), "");
        let third = parser.next().expect("an event").expect("no parse error");
        assert_eq!(
            String::from_utf8(third.id).unwrap(),
            "",
            "the cleared buffer stays cleared"
        );
    }

    // WHATWG "process the field": at most one space after the colon is
    // stripped. `data:  x` carries ` x`, and a space later in the value
    // is never touched.
    #[test]
    fn only_the_first_leading_space_is_stripped() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("data:  two spaces\n\ndata:a b\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.data).unwrap(), " two spaces");
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(second.data).unwrap(), "a b");
    }

    // WHATWG: CRLF, LF, and CR are all line terminators, and a CRLF pair
    // split across two frames is still one terminator.
    #[test]
    fn cr_and_crlf_terminate_lines() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 5\r\ndata: one\rdata: two\r"));
        parser.push_bytes(Vec::from("\r\r\ndata: three\r\n\r\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.id).unwrap(), "5");
        assert_eq!(String::from_utf8(first.data).unwrap(), "one\ntwo");
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(second.data).unwrap(), "three");
        assert!(parser.next().is_none());
    }

    // WHATWG: an `id` whose value contains U+0000 NULL is ignored - it is
    // the resume token a reconnect echoes into a `Last-Event-ID` header.
    #[test]
    fn an_id_containing_nul_is_ignored() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 9\ndata: a\n\nid: b\0ad\ndata: b\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.id).unwrap(), "9");
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(
            String::from_utf8(second.id).unwrap(),
            "9",
            "the poisoned id neither replaces nor clears the buffer"
        );
    }

    #[test]
    fn repeated_id_fields_replace_the_previous_value() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("id: 1\nid: 2\ndata: x\n\n"));

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.id).unwrap(), "2");
        assert_eq!(String::from_utf8(event.data).unwrap(), "x");
    }

    #[test]
    fn push_bytes_preserves_an_unconsumed_buffer() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from("data: one\n\ndata: two\n\n"));

        let first = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(first.data).unwrap(), "one");
        assert!(
            !parser.needs_bytes(),
            "the buffer still holds the second event"
        );

        parser.push_bytes(Vec::from("data: three\n\n"));
        let second = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(second.data).unwrap(), "two");
        let third = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(third.data).unwrap(), "three");
    }

    // WHATWG: a line's first character is part of the field name unless it
    // is a colon. A space-prefixed `data` line is therefore the unknown
    // field " data" (ignored), never a data field.
    #[test]
    fn a_leading_space_starts_a_field_name() {
        let mut parser = super::EventParser::default();
        parser.push_bytes(Vec::from(" data: skipped\ndata: kept\n\n"));

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.data).unwrap(), "kept");
        assert!(
            parser.next().is_none(),
            "the ignored field dispatched nothing"
        );
    }

    #[test]
    fn an_unbounded_comment_errors_once_and_resynchronises() {
        let mut parser = super::EventParser::default();
        let mut frame = Vec::from(":");
        frame.extend_from_slice(&vec![b'z'; super::MAX_EVENT_SIZE + 4]);
        frame.extend_from_slice(b"\n\ndata: recovered\n\n");
        parser.push_bytes(frame);

        assert!(parser.next().expect("an item").is_err());
        let recovered = parser
            .next()
            .expect("a recovered event")
            .expect("no parse error");
        assert_eq!(String::from_utf8(recovered.data).unwrap(), "recovered");
    }

    // A bounded comment does not trip the cap, and the counter resets for
    // the next comment line.
    #[test]
    fn bounded_comments_pass_and_reset_the_counter() {
        let mut parser = super::EventParser::default();
        let long_comment = format!(":{}\n", "c".repeat(super::MAX_EVENT_SIZE - 1));
        let mut frame = long_comment.clone().into_bytes();
        frame.extend_from_slice(long_comment.as_bytes());
        frame.extend_from_slice(b"data: alive\n\n");
        parser.push_bytes(frame);

        let event = parser.next().expect("an event").expect("no parse error");
        assert_eq!(String::from_utf8(event.data).unwrap(), "alive");
    }

    #[test]
    fn an_oversized_field_errors_once_and_resynchronises() {
        let mut parser = super::EventParser::default();
        let mut frame = vec![b'x'; super::MAX_EVENT_SIZE + 4];
        frame.extend_from_slice(b": v\n\ndata: recovered\n\n");
        parser.push_bytes(frame);

        assert!(parser.next().expect("an item").is_err());
        let recovered = parser
            .next()
            .expect("a recovered event")
            .expect("no parse error");
        assert_eq!(String::from_utf8(recovered.data).unwrap(), "recovered");
    }

    #[test]
    fn multi_line_data_is_capped_and_resynchronises() {
        let mut parser = super::EventParser::default();
        let line = format!("data: {}\n", "y".repeat(1024));
        let mut frame = String::new();
        for _ in 0..=(super::MAX_EVENT_SIZE / 1024) {
            frame.push_str(&line);
        }
        frame.push_str("\ndata: recovered\n\n");
        parser.push_bytes(frame.into_bytes());

        assert!(parser.next().expect("an item").is_err());
        let recovered = parser
            .next()
            .expect("a recovered event")
            .expect("no parse error");
        assert_eq!(String::from_utf8(recovered.data).unwrap(), "recovered");
    }
}
