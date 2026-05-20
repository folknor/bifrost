//! Public IMAP types.
//!
//! Core types from RFC 3501 (`IMAP4rev1`) and RFC 9051 (`IMAP4rev2`), plus
//! RFC 5322 (Internet Message Format), RFC 2045/2046 (MIME),
//! RFC 2183 (Content-Disposition), and extension RFCs.
//!
//! All types are fully owned  -  no lifetime parameters. Parsers produce these from `&[u8]`
//! and callers can store them freely.

mod address;
mod auth;
pub(crate) mod body;
mod command;
pub(crate) mod envelope;
mod events;
pub(crate) mod fetch;
pub(crate) mod flag;
mod ids;
pub(crate) mod mailbox;
pub(crate) mod notify;
mod profile;
pub(crate) mod response;
pub(crate) mod rfc2231;
pub mod search;
mod secret;
mod sync;
pub(crate) mod validated;

pub use address::Address;
pub use auth::{AuthMechanism, AuthOutcome, AuthPolicy, Credentials};
pub use body::{BodyStructure, ContentDisposition};
pub use envelope::{Envelope, EnvelopeAddress};
pub use events::EventImpact;
pub(crate) use fetch::format_fetch_attrs;
pub use fetch::{
    AppendMessage, BinarySection, BodySection, FetchAttr, FetchResponse, StoreOperation,
    StoreResult,
};
pub use flag::Flag;
pub use ids::{GmailMessageId, GmailThreadId, ModSeq, Seq, SeqSet, Uid, UidSet, UidValidity};
pub use mailbox::{
    MailboxAttribute, MailboxInfo, SelectedMailbox, SpecialUse, StatusItem, StatusResult,
};
pub use notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};
pub use profile::ServerProfile;
pub use response::{
    AclEntry, Capability, ContinuationRequest, CopyResult, EsearchResponse, ExpungeResult,
    GreetingResponse, GreetingStatus, ListRightsResponse, MetadataEntry, MetadataResult,
    MoveResult, NamespaceDescriptor, NamespaceResponse, QresyncParams, QuotaResource,
    QuotaRootResponse, Response, ResponseCode, SelectOptions, StatusKind, TaggedResponse,
    ThreadNode, UidRange, UntaggedResponse, UntaggedStatus,
};
pub use search::SearchCriteria;
pub use secret::{IntoSecretString, SecretString};
pub use sync::{SyncFetchRequest, SyncFetchResult, SyncSelectOptions, SyncSelectResult};
pub use validated::{ImapAtom, MailboxName, ObjectId, SequenceSet, ValidationError};

// Re-export command types for internal use only.
pub(crate) use command::{Command, CommandKind};
