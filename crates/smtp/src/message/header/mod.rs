//! Headers widely used in email messages

use std::{
    borrow::Cow,
    error::Error,
    fmt::{self, Display, Formatter},
    ops::Deref,
};

use bifrost_types::compose::Address as ComposeAddress;

// pub: users select typed headers from the message::header module.
pub use self::{
    content::*,
    content_disposition::ContentDisposition,
    content_type::{ContentType, ContentTypeErr},
    date::Date,
    mailbox::*,
    special::*,
    textual::*,
};
use crate::{BoxError, message::Mailboxes};

mod content;
mod content_disposition;
mod content_type;
mod date;
mod mailbox;
mod special;
mod textual;

/// Represents an email header
///
/// Email header as defined in [RFC5322](https://datatracker.ietf.org/doc/html/rfc5322) and extensions.
// pub: users implement and pass typed custom headers through MessageBuilder.
pub trait Header: Clone {
    fn name() -> HeaderName;

    fn parse(s: &str) -> Result<Self, BoxError>;

    /// Describe the value and its grammar. Never encodes: encoding, and
    /// refusal of an unrepresentable value, happens when the value enters a
    /// [`Headers`].
    fn display(&self) -> HeaderValue;
}

/// One stored field: its description and the validated bytes it encodes to.
#[derive(Debug, Clone)]
struct Entry {
    value: HeaderValue,
    /// The complete field without its terminating CRLF.
    field: String,
}

/// A set of email headers
///
/// Every entry is encoded and validated on insertion, so formatting a
/// `Headers` emits exactly the validated bytes and cannot fail.
#[derive(Debug, Clone, Default)]
// pub: users inspect and mutate headers on built messages.
pub struct Headers {
    headers: Vec<Entry>,
}

impl Headers {
    /// Create an empty `Headers`
    ///
    /// This function does not allocate.
    #[inline]
    pub const fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }

    /// Create an empty `Headers` with a pre-allocated capacity
    ///
    /// Pre-allocates a capacity of at least `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            headers: Vec::with_capacity(capacity),
        }
    }

    /// Returns a copy of a `Header` present in `Headers`
    ///
    /// Returns `None` if `Header` isn't present in `Headers`.
    pub fn get<H: Header>(&self) -> Option<H> {
        self.get_raw(&H::name())
            .and_then(|raw_value| H::parse(raw_value).ok())
    }

    /// Sets `Header` into `Headers`, overriding `Header` if it
    /// was already present in `Headers`
    ///
    /// # Errors
    ///
    /// `InvalidInput` when the value cannot be encoded within RFC 5322 line
    /// limits or holds octets its grammar forbids. The previous value, if
    /// any, is left in place.
    pub fn set<H: Header>(&mut self, header: H) -> Result<(), crate::error::Error> {
        self.insert_raw(header.display())
    }

    /// Test fixtures: insert a raw value that must encode.
    #[cfg(test)]
    pub(crate) fn insert_known(&mut self, value: HeaderValue) {
        self.insert_raw(value).expect("valid fixture header");
    }

    /// Insert a header built by this crate from constant or validated input,
    /// whose encoding cannot fail.
    pub(crate) fn set_known<H: Header>(&mut self, header: H) {
        self.set(header)
            .expect("a header built from constant or validated input encodes");
    }

    /// Remove `Header` from `Headers`, returning it
    ///
    /// Returns `None` if `Header` isn't in `Headers`.
    pub fn remove<H: Header>(&mut self) -> Option<H> {
        self.remove_raw(&H::name())
            .and_then(|value| H::parse(&value.raw_value).ok())
    }

    /// Clears `Headers`, removing all headers from it
    ///
    /// Any pre-allocated capacity is left untouched.
    #[inline]
    pub fn clear(&mut self) {
        self.headers.clear();
    }

    /// Returns a reference to the raw value of header `name`
    ///
    /// Returns `None` if `name` isn't present in `Headers`.
    pub fn get_raw(&self, name: &str) -> Option<&str> {
        self.find_header(name).map(|value| value.raw_value.as_str())
    }

    /// Inserts a raw header into `Headers`, overriding `value` if it
    /// was already present in `Headers`.
    ///
    /// # Errors
    ///
    /// As [`Headers::set`]: the value is encoded before anything is
    /// replaced, so a refused value leaves the previous entry untouched.
    pub fn insert_raw(&mut self, value: HeaderValue) -> Result<(), crate::error::Error> {
        let field = value.encode()?;
        let entry = Entry { value, field };
        match self
            .headers
            .iter_mut()
            .find(|current| entry.value.name == current.value.name)
        {
            Some(current) => *current = entry,
            None => self.headers.push(entry),
        }
        Ok(())
    }

    /// Remove a raw header from `Headers`, returning it
    ///
    /// Returns `None` if `name` isn't present in `Headers`.
    pub fn remove_raw(&mut self, name: &str) -> Option<HeaderValue> {
        self.headers
            .iter()
            .position(|entry| name == entry.value.name)
            .map(|i| self.headers.remove(i).value)
    }

    fn find_header(&self, name: &str) -> Option<&HeaderValue> {
        self.headers
            .iter()
            .map(|entry| &entry.value)
            .find(|value| name == value.name)
    }
}

impl Display for Headers {
    /// Formats `Headers`, ready to put them into an email
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for entry in &self.headers {
            f.write_str(&entry.field)?;
            f.write_str("\r\n")?;
        }

        Ok(())
    }
}

/// A possible error when converting a `HeaderName` from another type.
// comes from `http` crate
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
#[non_exhaustive]
// pub: users receive this from custom raw header name validation.
pub struct InvalidHeaderName;

impl fmt::Display for InvalidHeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid header name")
    }
}

impl Error for InvalidHeaderName {}

/// A valid header name
#[derive(Debug, Clone)]
// pub: users construct safe raw header names for HeaderValue.
pub struct HeaderName(Cow<'static, str>);

/// RFC 5322 `ftext`: `%d33-57 / %d59-126`, i.e. printable ASCII minus the
/// colon. Excluding the control range is what keeps CR and LF out of a header
/// name: `Display for Headers` writes the name verbatim ahead of `": "`, so a
/// name carrying CRLF would terminate the field and let the rest of the string
/// be read by the receiver as further headers - header injection from any
/// caller that builds a name out of untrusted input. The value side already
/// refuses 10 and 13 by RFC 2047 encoding them; this is the matching defence on
/// the name side. Subsumes the older space/colon check.
const fn is_ftext(byte: u8) -> bool {
    matches!(byte, 33..=57 | 59..=126)
}

impl HeaderName {
    /// Creates a new header name
    pub fn new_from_ascii(ascii: String) -> Result<Self, InvalidHeaderName> {
        if !ascii.is_empty() && ascii.len() <= 76 && ascii.bytes().all(is_ftext) {
            Ok(Self(Cow::Owned(ascii)))
        } else {
            Err(InvalidHeaderName)
        }
    }

    /// Creates a new header name, panics on invalid name
    pub const fn new_from_ascii_str(ascii: &'static str) -> Self {
        assert!(!ascii.is_empty());
        assert!(ascii.len() <= 76);

        let bytes = ascii.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            assert!(is_ftext(bytes[i]));

            i += 1;
        }

        Self(Cow::Borrowed(ascii))
    }
}

impl Display for HeaderName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

impl Deref for HeaderName {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for HeaderName {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        let s: &str = self.as_ref();
        s.as_bytes()
    }
}

impl AsRef<str> for HeaderName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<HeaderName> for HeaderName {
    fn eq(&self, other: &HeaderName) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<&str> for HeaderName {
    fn eq(&self, other: &&str) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<HeaderName> for &str {
    fn eq(&self, other: &HeaderName) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}

/// The grammar a [`HeaderValue`] declares for its text. The shared
/// `bifrost-mime` writer applies the matching encoding and folding rule.
#[derive(Debug, Clone, PartialEq)]
enum Grammar {
    Unstructured,
    MessageIds,
    PhraseList,
    ListId,
    Structured,
    Raw,
    /// Display name and addr-spec per mailbox.
    Addresses(Vec<(Option<String>, String)>),
    Parameterized {
        value: String,
        parameters: Vec<(String, String)>,
    },
    PreEncoded(String),
}

/// A header field as described by the caller: its name, the raw value typed
/// getters parse back, and the grammar it is encoded with.
///
/// Describing a value never fails. Encoding happens once, when the value
/// enters a [`Headers`] ([`Headers::set`], [`Headers::insert_raw`]), through
/// the shared `bifrost-mime` writer; that is where an unrepresentable value
/// (an indivisible token past 998 octets, a control octet in a structured
/// field) is refused, so a `Headers` only ever holds bytes that were
/// validated, and formatting it cannot fail.
#[derive(Debug, Clone, PartialEq)]
// pub: users insert raw custom headers through MessageBuilder.
pub struct HeaderValue {
    name: HeaderName,
    raw_value: String,
    grammar: Grammar,
}

impl HeaderValue {
    /// A value whose grammar the caller does not declare: its own wire
    /// spelling. Legal wire text (printable ASCII and WSP, CRLF only as a
    /// fold before WSP, lines within 998 octets) is emitted byte-for-byte;
    /// otherwise words holding non-ASCII or control octets become RFC 2047
    /// encoded words and the value folds at 78 octets. An ASCII word is never
    /// encoded, since the grammar may forbid it, so one too long for 998
    /// octets is refused at insertion.
    pub fn new(name: HeaderName, raw_value: String) -> Self {
        Self::with_grammar(name, raw_value, Grammar::Raw)
    }

    /// RFC 5322 unstructured text (Subject-like): RFC 2047 encodes non-ASCII,
    /// control octets and overlong words.
    pub fn unstructured(name: HeaderName, text: String) -> Self {
        Self::with_grammar(name, text, Grammar::Unstructured)
    }

    /// Space-separated message identifiers, already bracketed.
    pub fn message_ids(name: HeaderName, ids: String) -> Self {
        Self::with_grammar(name, ids, Grammar::MessageIds)
    }

    /// Comma-separated RFC 5322 phrases.
    pub fn phrase_list(name: HeaderName, text: String) -> Self {
        Self::with_grammar(name, text, Grammar::PhraseList)
    }

    /// Optional display phrase and an angle-bracketed list identifier.
    pub fn list_id(name: HeaderName, text: String) -> Self {
        Self::with_grammar(name, text, Grammar::ListId)
    }

    /// An ASCII structured value emitted verbatim and folded at whitespace;
    /// never encoded, controls and non-ASCII refused.
    pub fn structured(name: HeaderName, value: String) -> Self {
        Self::with_grammar(name, value, Grammar::Structured)
    }

    /// An address list. The raw value is the parseable `Mailboxes` display.
    pub fn mailboxes(name: HeaderName, mailboxes: &Mailboxes) -> Self {
        let addresses = mailboxes
            .iter()
            .map(|mailbox| (mailbox.name.clone(), mailbox.email.to_string()))
            .collect();
        Self::with_grammar(name, mailboxes.to_string(), Grammar::Addresses(addresses))
    }

    /// A MIME value with parameters; long or non-ASCII parameter values use
    /// RFC 2231 continuations. `raw_value` is what the typed getter parses.
    pub fn parameterized(
        name: HeaderName,
        raw_value: String,
        value: String,
        parameters: Vec<(String, String)>,
    ) -> Self {
        Self::with_grammar(
            name,
            raw_value,
            Grammar::Parameterized { value, parameters },
        )
    }

    /// Construct a new `HeaderValue` using a pre-encoded header value.
    ///
    /// Only framing is validated at insertion (printable ASCII and WSP, CRLF
    /// only before WSP, every line within 998 octets); the bytes are emitted
    /// as given. That `raw_value` means the same thing as `encoded_value`
    /// remains the caller's responsibility, which is why the name says
    /// dangerous: typed getters parse `raw_value`, recipients read
    /// `encoded_value`.
    pub fn dangerous_new_pre_encoded(
        name: HeaderName,
        raw_value: String,
        encoded_value: String,
    ) -> Self {
        Self::with_grammar(name, raw_value, Grammar::PreEncoded(encoded_value))
    }

    fn with_grammar(name: HeaderName, raw_value: String, grammar: Grammar) -> Self {
        Self {
            name,
            raw_value,
            grammar,
        }
    }

    /// Encode through the shared writer: the complete field, without CRLF.
    fn encode(&self) -> Result<String, crate::error::Error> {
        use bifrost_mime::HeaderValue as Wire;
        let raw = self.raw_value.as_str();
        let parameters: Vec<(&str, &str)>;
        let addresses: Vec<ComposeAddress>;
        let value = match &self.grammar {
            Grammar::Unstructured => Wire::Unstructured(raw),
            Grammar::MessageIds => Wire::MessageIds(raw),
            Grammar::PhraseList => Wire::PhraseList(raw),
            Grammar::ListId => Wire::ListId(raw),
            Grammar::Structured => Wire::Structured(raw),
            Grammar::Raw => Wire::Raw(raw),
            Grammar::Addresses(owned) => {
                addresses = owned
                    .iter()
                    .map(|(name, address)| ComposeAddress {
                        name: name.clone(),
                        address: address.clone(),
                    })
                    .collect();
                Wire::Addresses(&addresses)
            }
            Grammar::Parameterized {
                value,
                parameters: owned,
            } => {
                parameters = owned
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                Wire::Parameterized {
                    value,
                    parameters: &parameters,
                }
            }
            Grammar::PreEncoded(encoded) => Wire::PreEncoded(encoded),
        };
        bifrost_mime::write_header(&self.name, value).map_err(|error| {
            crate::error::Error::InvalidInput(format!(
                "{} header cannot be encoded: {error}",
                &*self.name
            ))
        })
    }
}

#[cfg(test)]
mod tests {

    use super::{HeaderName, HeaderValue, Headers, Subject, To};
    use crate::message::Mailboxes;

    #[test]
    fn valid_headername() {
        assert!(HeaderName::new_from_ascii(String::from("From")).is_ok());
    }

    #[test]
    fn non_ascii_headername() {
        assert!(HeaderName::new_from_ascii(String::from("\u{1F30E}")).is_err());
    }

    #[test]
    fn spaces_in_headername() {
        assert!(HeaderName::new_from_ascii(String::from("From ")).is_err());
    }

    #[test]
    fn colons_in_headername() {
        assert!(HeaderName::new_from_ascii(String::from("From:")).is_err());
    }

    #[test]
    fn control_characters_in_headername() {
        // Header injection: the name is written verbatim by `Display`, so a
        // CRLF here would open a second header field on the wire.
        assert!(HeaderName::new_from_ascii(String::from("X-Foo\r\nBcc")).is_err());
        assert!(HeaderName::new_from_ascii(String::from("X-Foo\n")).is_err());
        assert!(HeaderName::new_from_ascii(String::from("X-Foo\r")).is_err());
        assert!(HeaderName::new_from_ascii(String::from("X-Foo\t")).is_err());
        assert!(HeaderName::new_from_ascii(String::from("X-\u{0}Foo")).is_err());
    }

    #[test]
    #[should_panic]
    fn const_control_characters_in_headername() {
        let _ = HeaderName::new_from_ascii_str("X-Foo\r\nBcc");
    }

    #[test]
    fn empty_headername() {
        assert!(HeaderName::new_from_ascii(String::new()).is_err());
    }

    #[test]
    fn const_valid_headername() {
        let _ = HeaderName::new_from_ascii_str("From");
    }

    #[test]
    #[should_panic]
    fn const_non_ascii_headername() {
        let _ = HeaderName::new_from_ascii_str("\u{1F30E}");
    }

    #[test]
    #[should_panic]
    fn const_spaces_in_headername() {
        let _ = HeaderName::new_from_ascii_str("From ");
    }

    #[test]
    #[should_panic]
    fn const_colons_in_headername() {
        let _ = HeaderName::new_from_ascii_str("From:");
    }

    #[test]
    #[should_panic]
    fn const_empty_headername() {
        let _ = HeaderName::new_from_ascii_str("");
    }

    #[test]
    fn headername_headername_eq() {
        assert_eq!(
            HeaderName::new_from_ascii_str("From"),
            HeaderName::new_from_ascii_str("From")
        );
    }

    #[test]
    fn headername_str_eq() {
        assert_eq!(HeaderName::new_from_ascii_str("From"), "From");
    }

    #[test]
    fn str_headername_eq() {
        assert_eq!("From", HeaderName::new_from_ascii_str("From"));
    }

    #[test]
    fn headername_headername_eq_case_insensitive() {
        assert_eq!(
            HeaderName::new_from_ascii_str("From"),
            HeaderName::new_from_ascii_str("from")
        );
    }

    #[test]
    fn headername_str_eq_case_insensitive() {
        assert_eq!(HeaderName::new_from_ascii_str("From"), "from");
    }

    #[test]
    fn str_headername_eq_case_insensitive() {
        assert_eq!("from", HeaderName::new_from_ascii_str("From"));
    }

    #[test]
    fn headername_headername_ne() {
        assert_ne!(
            HeaderName::new_from_ascii_str("From"),
            HeaderName::new_from_ascii_str("To")
        );
    }

    #[test]
    fn headername_str_ne() {
        assert_ne!(HeaderName::new_from_ascii_str("From"), "To");
    }

    #[test]
    fn str_headername_ne() {
        assert_ne!("From", HeaderName::new_from_ascii_str("To"));
    }

    // names taken randomly from https://it.wikipedia.org/wiki/Pinco_Pallino

    #[test]
    fn format_ascii() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            "John Doe <example@example.com>, Jean Dupont <jean@example.com>".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            "To: John Doe <example@example.com>, Jean Dupont <jean@example.com>\r\n"
        );
    }

    /// A raw value that is already legal wire text is the caller's own
    /// spelling and is kept byte-for-byte, even past the 78-octet
    /// preference; typed headers fold (see `typed_subject_folds`).
    #[test]
    fn legal_raw_ascii_values_are_kept_verbatim() {
        for (name, value) in [
            (
                "To",
                "Ascii <example@example.com>, John Doe <johndoe@example.com, John Smith <johnsmith@example.com>, Pinco Pallino <pincopallino@example.com>, Jemand <jemand@example.com>, Jean Dupont <jean@example.com>",
            ),
            (
                "Subject",
                "Hello! This is bifrost_smtp, and this IsAVeryLongLineDoYouKnowWhatsGoingToHappenIGuessWeAreGoingToFindOut. Ok I guess that's it!",
            ),
            ("Subject", "  padded\r\n\tand  folded  "),
        ] {
            let mut headers = Headers::new();
            headers.insert_known(HeaderValue::new(
                HeaderName::new_from_ascii(name.to_owned()).unwrap(),
                value.to_owned(),
            ));
            assert_eq!(headers.to_string(), format!("{name}: {value}\r\n"));
        }
    }

    #[test]
    fn typed_subject_folds() {
        let mut headers = Headers::new();
        headers.set_known(Subject::from(
            "Hello! This is bifrost_smtp, and this IsAVeryLongLineDoYouKnowWhatsGoingToHappenIGuessWeAreGoingToFindOut. Ok I guess that's it!".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            concat!(
                "Subject: Hello! This is bifrost_smtp, and this\r\n",
                " IsAVeryLongLineDoYouKnowWhatsGoingToHappenIGuessWeAreGoingToFindOut. Ok I\r\n",
                " guess that's it!\r\n"
            )
        );
    }

    /// The hard limit: a raw ASCII token that cannot fit 998 octets is
    /// refused, never written overlong, and the previous value survives.
    #[test]
    fn overlong_raw_token_is_refused_and_keeps_the_old_value() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Token"),
            "short".to_owned(),
        ));
        assert!(
            headers
                .insert_raw(HeaderValue::new(
                    HeaderName::new_from_ascii_str("X-Token"),
                    "t".repeat(1000),
                ))
                .is_err()
        );
        assert_eq!(headers.to_string(), "X-Token: short\r\n");
        // A non-ASCII word beside it forces the encoding path, which still
        // never encodes the ASCII token.
        assert!(
            headers
                .insert_raw(HeaderValue::new(
                    HeaderName::new_from_ascii_str("X-Token"),
                    format!("\u{e9} {}", "t".repeat(1000)),
                ))
                .is_err()
        );
    }

    #[test]
    fn format_ascii_with_folding_giant_word() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Subject"),
            "1abcdefghijklmnopqrstuvwxyz2abcdefghijklmnopqrstuvwxyz3abcdefghijklmnopqrstuvwxyz4abcdefghijklmnopqrstuvwxyz5abcdefghijklmnopqrstuvwxyz6abcdefghijklmnopqrstuvwxyz".to_owned()
        ));

        assert_eq!(
            headers.to_string(),
            "Subject: 1abcdefghijklmnopqrstuvwxyz2abcdefghijklmnopqrstuvwxyz3abcdefghijklmnopqrstuvwxyz4abcdefghijklmnopqrstuvwxyz5abcdefghijklmnopqrstuvwxyz6abcdefghijklmnopqrstuvwxyz\r\n",
        );
    }

    /// Long non-ASCII display names fold between encoded words on typed and
    /// raw address paths. The ASCII-token example below overflows 78 but fits
    /// 998; a token past 998 is refused (see
    /// `overlong_raw_token_is_refused_and_keeps_the_old_value`).
    #[test]
    fn long_non_ascii_display_names_fold_on_every_address_path() {
        fn longest_line(headers: &Headers) -> usize {
            headers
                .to_string()
                .split("\r\n")
                .map(str::len)
                .max()
                .expect("at least one line")
        }

        let name = "Иванов".repeat(100);

        let mut typed = Headers::new();
        typed.set_known(To::from(Mailboxes::from_iter([format!(
            "{name} <long@example.com>"
        )
        .parse()
        .expect("mailbox parses")])));
        assert!(longest_line(&typed) <= 78, "typed To path did not fold");

        let mut raw = Headers::new();
        raw.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            format!("{name} <long@example.com>"),
        ));
        assert!(longest_line(&raw) <= 78, "raw To path did not fold");

        // The unfoldable case: a giant all-ASCII token beside an encoded word.
        let mut giant_token = Headers::new();
        giant_token.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            format!("{}, Ltd \u{e9} <long@example.com>", "Ivanov".repeat(100)),
        ));
        let longest = longest_line(&giant_token);
        assert!(
            longest > 78,
            "the giant-token case is the one that overflows"
        );
        assert!(longest < 998, "this fixture is shorter than the hard limit");
    }

    #[test]
    fn format_special() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            "Seán <sean@example.com>".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            "To: =?UTF-8?B?U2XDoW4=?= <sean@example.com>\r\n"
        );
    }

    #[test]
    fn format_special_emoji() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            "\u{1F30E} <world@example.com>".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            "To: =?UTF-8?B?8J+Mjg==?= <world@example.com>\r\n"
        );
    }

    #[test]
    fn format_special_with_folding() {
        let mut headers = Headers::new();
        let to = To::from(Mailboxes::from_iter([
            "\u{1F30D} <world@example.com>".parse().unwrap(),
            "\u{1F986} Everywhere <ducks@example.com>".parse().unwrap(),
            "Иванов Иван Иванович <ivanov@example.com>".parse().unwrap(),
            "Jānis Bērziņš <janis@example.com>".parse().unwrap(),
            "Seán Ó Rudaí <sean@example.com>".parse().unwrap(),
        ]));
        headers.set_known(to.clone());

        let wire = headers.to_string();
        assert!(wire.split("\r\n").all(|line| line.len() <= 78), "{wire}");
        let parsed = bifrost_mime::HeaderMap::parse(wire.as_bytes()).addresses("To");
        let expected: Vec<(Option<String>, String)> =
            to.0.iter()
                .map(|mailbox| (mailbox.name.clone(), mailbox.email.to_string()))
                .collect();
        let parsed: Vec<(Option<String>, String)> = parsed
            .into_iter()
            .map(|address| (address.name, address.address))
            .collect();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn format_special_with_folding_raw() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            "\u{1F30D} <world@example.com>, \u{1F986} Everywhere <ducks@example.com>, Иванов Иван Иванович <ivanov@example.com>, Jānis Bērziņš <janis@example.com>, Seán Ó Rudaí <sean@example.com>".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            concat!(
                "To: =?UTF-8?B?8J+MjQ==?= <world@example.com>, =?UTF-8?B?8J+mhg==?=\r\n",
                " Everywhere <ducks@example.com>,\r\n",
                " =?UTF-8?B?0JjQstCw0L3QvtCyINCY0LLQsNC9INCY0LLQsNC90L7QstC40Yc=?=\r\n",
                " <ivanov@example.com>, =?UTF-8?B?SsSBbmlzIELEk3J6acWGxaE=?=\r\n",
                " <janis@example.com>, =?UTF-8?B?U2XDoW4gw5MgUnVkYcOt?= <sean@example.com>\r\n",
            )
        );
    }

    #[test]
    fn format_slice_on_char_boundary_bug() {
        let mut headers = Headers::new();
        let text = "\u{1F973}".repeat(60);
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Subject"),
            text.clone(),
        ));

        let wire = headers.to_string();
        assert!(wire.split("\r\n").all(|line| line.len() <= 78), "{wire}");
        let value = wire
            .trim_start_matches("Subject:")
            .trim_end()
            .replace("\r\n ", " ");
        assert_eq!(
            bifrost_mime::decode_encoded_words(value.trim().as_bytes()),
            text
        );
    }

    #[test]
    fn format_bad_stuff() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Subject"),
            "Hello! \r\n This is \" bad \0. \u{1F44B}".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            "Subject: Hello! =?UTF-8?B?DQo=?= This is \" bad =?UTF-8?B?AC4g8J+Riw==?=\r\n"
        );
    }

    #[test]
    fn format_everything() {
        let mut headers = Headers::new();
        headers.insert_known(
            HeaderValue::new(
            HeaderName::new_from_ascii_str("Subject"),
            "Hello! This is bifrost_smtp, and this IsAVeryLongLineDoYouKnowWhatsGoingToHappenIGuessWeAreGoingToFindOut. Ok I guess that's it!".to_owned()
            )
        );
        headers.insert_known(
            HeaderValue::new(
            HeaderName::new_from_ascii_str("To"),
            "\u{1F30D} <world@example.com>, \u{1F986} Everywhere <ducks@example.com>, Иванов Иван Иванович <ivanov@example.com>, Jānis Bērziņš <janis@example.com>, Seán Ó Rudaí <sean@example.com>".to_owned(),
            )
        );
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("From"),
            "Someone <somewhere@example.com>".to_owned(),
        ));
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Content-Transfer-Encoding"),
            "quoted-printable".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            concat!(
                "Subject: Hello! This is bifrost_smtp, and this IsAVeryLongLineDoYouKnowWhatsGoingToHappenIGuessWeAreGoingToFindOut. Ok I guess that's it!\r\n",
                "To: =?UTF-8?B?8J+MjQ==?= <world@example.com>, =?UTF-8?B?8J+mhg==?=\r\n",
                " Everywhere <ducks@example.com>,\r\n",
                " =?UTF-8?B?0JjQstCw0L3QvtCyINCY0LLQsNC9INCY0LLQsNC90L7QstC40Yc=?=\r\n",
                " <ivanov@example.com>, =?UTF-8?B?SsSBbmlzIELEk3J6acWGxaE=?=\r\n",
                " <janis@example.com>, =?UTF-8?B?U2XDoW4gw5MgUnVkYcOt?= <sean@example.com>\r\n",
                "From: Someone <somewhere@example.com>\r\n",
                "Content-Transfer-Encoding: quoted-printable\r\n",
            )
        );
    }

    #[test]
    fn issue_653() {
        let mut headers = Headers::new();
        headers.insert_known(HeaderValue::new(
            HeaderName::new_from_ascii_str("Subject"),
            "＋仮名 :a;go; ;;;;;s;;;;;;;;;;;;;;;;fffeinmjgggggggggｆっ".to_owned(),
        ));

        assert_eq!(
            headers.to_string(),
            concat!(
                "Subject: =?UTF-8?B?77yL5Luu5ZCN?= :a;go;\r\n",
                " =?UTF-8?B?Ozs7OztzOzs7Ozs7Ozs7Ozs7Ozs7O2ZmZmVpbm1qZ2dnZ2dnZ2dn772G44Gj?=\r\n",
            )
        );
    }
}
