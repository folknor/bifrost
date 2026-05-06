pub type ContactCardParseResponse =
    crate::core::parse::ParseResponse<Vec<super::ContactCard>>;

crate::define_parse_method!(
    ContactCardParseRequest,
    super::Property,
    "ContactCard/parse",
    crate::core::capability::ContactsParse,
    ContactCardParseResponse
);
