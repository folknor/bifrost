pub type CalendarEventParseResponse =
    crate::core::parse::ParseResponse<Vec<super::CalendarEvent>>;

crate::define_parse_method!(
    CalendarEventParseRequest,
    super::Property,
    "CalendarEvent/parse",
    crate::core::capability::CalendarsParse,
    CalendarEventParseResponse
);
