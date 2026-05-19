//! Public IMAP types.
//!
//! Core types from RFC 3501 (`IMAP4rev1`) and RFC 9051 (`IMAP4rev2`), plus
//! RFC 5322 (Internet Message Format), RFC 2045/2046 (MIME),
//! RFC 2183 (Content-Disposition), and extension RFCs.
//!
//! All types are fully owned  -  no lifetime parameters. Parsers produce these from `&[u8]`
//! and callers can store them freely.

mod address;
pub(crate) mod body;
mod command;
pub(crate) mod envelope;
pub(crate) mod fetch;
pub(crate) mod flag;
pub(crate) mod mailbox;
pub(crate) mod notify;
pub(crate) mod response;
pub(crate) mod rfc2231;
pub mod search;
pub(crate) mod validated;

pub use address::Address;
pub use body::{BodyStructure, ContentDisposition};
pub use envelope::{Envelope, EnvelopeAddress};
pub(crate) use fetch::format_fetch_attrs;
pub use fetch::{
    AppendMessage, BinarySection, BodySection, FetchAttr, FetchResponse, StoreOperation,
    StoreResult,
};
pub use flag::Flag;
pub use mailbox::{
    MailboxAttribute, MailboxInfo, SelectedMailbox, SpecialUse, StatusItem, StatusResult,
};
pub use notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};
pub use response::{
    AclEntry, Capability, ContinuationRequest, CopyResult, EsearchResponse, ExpungeResult,
    GreetingResponse, GreetingStatus, ListRightsResponse, MetadataEntry, MetadataResult,
    MoveResult, NamespaceDescriptor, NamespaceResponse, QresyncParams, QuotaResource,
    QuotaRootResponse, Response, ResponseCode, SelectOptions, StatusKind, TaggedResponse,
    ThreadNode, UidRange, UntaggedResponse, UntaggedStatus,
};
pub use search::SearchCriteria;
pub use validated::{ImapAtom, MailboxName, ObjectId, SequenceSet, ValidationError};

// Re-export command types for internal use only.
pub(crate) use command::{Command, CommandKind, SecretString};
