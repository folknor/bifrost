use std::{borrow::Cow, iter::FusedIterator, slice};

use chrono::{DateTime, FixedOffset};
use imap_proto::types::{
    AttributeValue, BodyStructure, Envelope, MessageSection, Response, SectionPath,
};

use super::{Flag, Seq, Uid};
use crate::types::ResponseData;

/// Format of Date and Time as defined RFC3501.
/// See `date-time` element in [Formal Syntax](https://tools.ietf.org/html/rfc3501#section-9)
/// chapter of this RFC.
const DATE_TIME_FORMAT: &str = "%d-%b-%Y %H:%M:%S %z";

/// An IMAP [`FETCH` response](https://tools.ietf.org/html/rfc3501#section-7.4.2) that contains
/// data about a particular message.
///
/// This response occurs as the result of a `FETCH` or `STORE`
/// command, as well as by unilateral server decision (e.g., flag updates).
#[derive(Debug)]
pub struct Fetch {
    response: ResponseData,
    /// The ordinal number of this message in its containing mailbox.
    pub message: Seq,

    /// A number expressing the unique identifier of the message.
    /// Only present if `UID` was specified in the query argument to `FETCH` and the server
    /// supports UIDs.
    pub uid: Option<Uid>,

    /// A number expressing the [RFC-2822](https://tools.ietf.org/html/rfc2822) size of the message.
    /// Only present if `RFC822.SIZE` was specified in the query argument to `FETCH`.
    pub size: Option<u32>,

    /// A number expressing the [RFC-7162](https://tools.ietf.org/html/rfc7162) mod-sequence
    /// of the message.
    pub modseq: Option<u64>,
}

/// A `FLAGS` attribute returned in a `FETCH` response.
///
/// This distinguishes `FLAGS ()` from a `FETCH` response that did not include a
/// `FLAGS` attribute at all.
///
/// `Flags` borrows from the underlying [`Fetch`] and cannot outlive it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Flags<'a> {
    raw: &'a [Cow<'a, str>],
}

/// Iterator over a [`Flags`] attribute.
#[derive(Clone, Debug)]
pub struct FlagsIter<'a> {
    raw: slice::Iter<'a, Cow<'a, str>>,
}

impl<'a> Iterator for FlagsIter<'a> {
    type Item = Flag<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next().map(|s| Flag::from(s.as_ref()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint()
    }
}

impl ExactSizeIterator for FlagsIter<'_> {}

impl FusedIterator for FlagsIter<'_> {}

impl<'a> Flags<'a> {
    /// Iterate over the flags in this `FLAGS` attribute.
    pub fn iter(&self) -> FlagsIter<'a> {
        FlagsIter {
            raw: self.raw.iter(),
        }
    }

    /// Returns how many flags are present in this `FLAGS` attribute.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Returns true if this is an empty `FLAGS ()` attribute.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }
}

impl<'a> IntoIterator for Flags<'a> {
    type IntoIter = FlagsIter<'a>;
    type Item = Flag<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &Flags<'a> {
    type IntoIter = FlagsIter<'a>;
    type Item = Flag<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Fetch {
    pub(crate) fn new(response: ResponseData) -> Self {
        let (message, uid, size, modseq) =
            if let Response::Fetch(message, attrs) = response.parsed() {
                let mut uid = None;
                let mut size = None;
                let mut modseq = None;

                for attr in attrs {
                    match attr {
                        AttributeValue::Uid(id) => uid = Some(*id),
                        AttributeValue::Rfc822Size(sz) => size = Some(*sz),
                        AttributeValue::ModSeq(ms) => modseq = Some(*ms),
                        _ => {}
                    }
                }
                (*message, uid, size, modseq)
            } else {
                unreachable!()
            };

        Fetch {
            response,
            message,
            uid,
            size,
            modseq,
        }
    }

    fn raw_flags(&self) -> Option<&[Cow<'_, str>]> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs.iter().find_map(|attr| match attr {
                AttributeValue::Flags(raw_flags) => Some(raw_flags.as_slice()),
                _ => None,
            })
        } else {
            unreachable!()
        }
    }

    /// A list of flags that are set for this message.
    ///
    /// This returns an empty iterator both when the server returned `FLAGS ()`
    /// and when the `FETCH` response did not include a `FLAGS` attribute. Use
    /// [`Fetch::flags_attribute`] when that distinction matters.
    pub fn flags(&self) -> impl Iterator<Item = Flag<'_>> {
        self.raw_flags()
            .into_iter()
            .flatten()
            .map(|s| Flag::from(s.as_ref()))
    }

    /// The `FLAGS` attribute in this `FETCH` response, if one was returned.
    pub fn flags_attribute(&self) -> Option<Flags<'_>> {
        self.raw_flags().map(|raw| Flags { raw })
    }

    /// The bytes that make up the header of this message, if `BODY[HEADER]`, `BODY.PEEK[HEADER]`,
    /// or `RFC822.HEADER` was included in the `query` argument to `FETCH`.
    pub fn header(&self) -> Option<&[u8]> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::BodySection {
                        section: Some(SectionPath::Full(MessageSection::Header)),
                        data: Some(hdr),
                        ..
                    }
                    | AttributeValue::Rfc822Header(Some(hdr)) => Some(hdr.as_ref()),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// The bytes that make up this message, included if `BODY[]` or `RFC822` was included in the
    /// `query` argument to `FETCH`. The bytes SHOULD be interpreted by the client according to the
    /// content transfer encoding, body type, and subtype.
    pub fn body(&self) -> Option<&[u8]> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::BodySection {
                        section: None,
                        data: Some(body),
                        ..
                    }
                    | AttributeValue::Rfc822(Some(body)) => Some(body.as_ref()),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// The bytes that make up the text of this message, included if `BODY[TEXT]`, `RFC822.TEXT`,
    /// or `BODY.PEEK[TEXT]` was included in the `query` argument to `FETCH`. The bytes SHOULD be
    /// interpreted by the client according to the content transfer encoding, body type, and
    /// subtype.
    pub fn text(&self) -> Option<&[u8]> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::BodySection {
                        section: Some(SectionPath::Full(MessageSection::Text)),
                        data: Some(body),
                        ..
                    }
                    | AttributeValue::Rfc822Text(Some(body)) => Some(body.as_ref()),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// The envelope of this message, if `ENVELOPE` was included in the `query` argument to
    /// `FETCH`. This is computed by the server by parsing the
    /// [RFC-2822](https://tools.ietf.org/html/rfc2822) header into the component parts, defaulting
    /// various fields as necessary.
    ///
    /// The full description of the format of the envelope is given in [RFC 3501 section
    /// 7.4.2](https://tools.ietf.org/html/rfc3501#section-7.4.2).
    pub fn envelope(&self) -> Option<&Envelope<'_>> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::Envelope(env) => Some(&**env),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// Extract the bytes that makes up the given `BOD[<section>]` of a `FETCH` response.
    ///
    /// See [section 7.4.2 of RFC 3501](https://tools.ietf.org/html/rfc3501#section-7.4.2) for
    /// details.
    pub fn section(&self, path: &SectionPath) -> Option<&[u8]> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::BodySection {
                        section: Some(sp),
                        data: Some(data),
                        ..
                    } if sp == path => Some(data.as_ref()),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// Extract the `INTERNALDATE` of a `FETCH` response
    ///
    /// See [section 2.3.3 of RFC 3501](https://tools.ietf.org/html/rfc3501#section-2.3.3) for
    /// details.
    pub fn internal_date(&self) -> Option<DateTime<FixedOffset>> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::InternalDate(date_time) => Some(date_time.as_ref()),
                    _ => None,
                })
                .next()
                .and_then(|date_time| DateTime::parse_from_str(date_time, DATE_TIME_FORMAT).ok())
        } else {
            unreachable!()
        }
    }

    /// Extract the `BODYSTRUCTURE` of a `FETCH` response
    ///
    /// See [section 2.3.6 of RFC 3501](https://tools.ietf.org/html/rfc3501#section-2.3.6) for
    /// details.
    pub fn bodystructure(&self) -> Option<&BodyStructure<'_>> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::BodyStructure(bs) => Some(bs),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// Extract the `X-GM-LABELS` of a `FETCH` response
    ///
    /// See [Access to Gmail labels: X-GM-LABELS](https://developers.google.com/gmail/imap/imap-extensions#access_to_labels_x-gm-labels)
    /// for details.
    pub fn gmail_labels(&self) -> Option<&Vec<Cow<'_, str>>> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::GmailLabels(gl) => Some(gl),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// Extract the `X-GM-MSGID` of a `FETCH` response
    ///
    /// See [Access to the Gmail unique message ID: X-GM-MSGID](https://developers.google.com/workspace/gmail/imap/imap-extensions#access_to_the_unique_message_id_x-gm-msgid)
    /// for details.
    pub fn gmail_msg_id(&self) -> Option<&u64> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::GmailMsgId(id) => Some(id),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }

    /// Extract the `X-GM-THRID` of a `FETCH` response
    ///
    /// See [Access to the Gmail thread ID: X-GM-THRID](https://developers.google.com/workspace/gmail/imap/imap-extensions#access_to_the_gmail_thread_id_x-gm-thrid)
    /// for details.
    pub fn gmail_thr_id(&self) -> Option<&u64> {
        if let Response::Fetch(_, attrs) = self.response.parsed() {
            attrs
                .iter()
                .filter_map(|av| match av {
                    AttributeValue::GmailThrId(id) => Some(id),
                    _ => None,
                })
                .next()
        } else {
            unreachable!()
        }
    }
}
