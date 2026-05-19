//! Pipeline builder for batching multiple pipelinable IMAP commands.
//!
//! Only commands that are safe to pipeline per RFC 3501 Section5.5 have
//! methods on [`Pipeline`]. State-changing commands (SELECT, STARTTLS,
//! IDLE, LOGOUT, etc.) intentionally lack pipeline methods.

use crate::types::{
    Flag, MailboxAttribute, MailboxName, NotifySetParams, SequenceSet, StoreOperation,
};

// ============================================================================
// Pipeline builder with typed handle tuple
// ============================================================================

use std::any::Any;
use std::marker::PhantomData;

use crate::error::Error;
use crate::types::response::{
    AclEntry, EsearchResponse, ListRightsResponse, MetadataResult, NamespaceResponse,
    QuotaResource, QuotaRootResponse, ThreadNode, UidRange,
};
use crate::types::validated::ParsedUidSet;
use crate::types::{
    Capability, Command, CopyResult, ExpungeResult, FetchResponse, MoveResult, StatusItem,
    StatusResult, StoreResult,
};

use super::MailboxInfo;
use super::SearchResult;
use super::dispatch;
use super::driver::ConsumerErased;

// ---------------------------------------------------------------------------
// PipelineError
// ---------------------------------------------------------------------------

/// Error returned by [`Pipeline::execute`] and [`Pipeline::execute_dynamic`].
///
/// Distinguished from [`Error`] because pipeline execution has three
/// failure classes: per-command errors (embedded in the result tuple as
/// `Result<T, Error>`), pipeline-level driver errors (encoding failure
/// that aborts the whole batch), and type-mismatch bugs (internal only).
#[derive(Debug)]
pub enum PipelineError {
    /// The driver task has exited  -  the command channel is closed.
    Disconnected,
    /// The driver returned a pipeline-level error (e.g., encoding
    /// failure that aborted the entire batch before any bytes were
    /// written to the wire).
    Driver(Error),
    /// Internal error: a consumer returned a type that does not match
    /// the expected downcast target. This indicates a bug in the
    /// pipeline builder's command-to-consumer mapping.
    TypeMismatch {
        /// Zero-based index of the command whose output could not be
        /// downcast.
        index: usize,
    },
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "pipeline: driver task disconnected"),
            Self::Driver(e) => write!(f, "pipeline: driver error: {e}"),
            Self::TypeMismatch { index } => {
                write!(f, "pipeline: type mismatch at command index {index}")
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(e) => Some(e),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// UnfoldTuple
// ---------------------------------------------------------------------------

/// Trait for converting a `Vec<Result<Box<dyn Any + Send>, Error>>`
/// (the driver's per-command results) into a flat, typed tuple.
///
/// Each impl maps a nested type-level list `(C, (B, (A, ())))` to its
/// corresponding flat tuple `(Result<A, Error>, Result<B, Error>, Result<C, Error>)`.
/// The nesting order matches the push order: the first command pushed
/// is the innermost type, and results are extracted in push order from
/// index 0 upward.
///
/// Impls are provided for 0 through 8 commands.
pub trait UnfoldTuple {
    /// The flat tuple type produced by unfolding.
    type Output;

    /// Consume the results vec and produce the typed tuple.
    ///
    /// Returns `Err(PipelineError::TypeMismatch)` if any `Ok` result
    /// fails to downcast to the expected type. Per-command `Err` values
    /// are preserved as-is in the tuple elements.
    fn unfold(
        results: Vec<Result<Box<dyn Any + Send>, Error>>,
    ) -> Result<Self::Output, PipelineError>;
}

/// Downcast a single result entry, preserving per-command errors.
fn downcast_result<T: Any + Send>(
    result: Result<Box<dyn Any + Send>, Error>,
    index: usize,
) -> Result<Result<T, Error>, PipelineError> {
    match result {
        Ok(boxed) => match boxed.downcast::<T>() {
            Ok(val) => Ok(Ok(*val)),
            Err(_) => Err(PipelineError::TypeMismatch { index }),
        },
        Err(e) => Ok(Err(e)),
    }
}

/// Empty pipeline  -  no commands.
impl UnfoldTuple for () {
    type Output = ();
    fn unfold(results: Vec<Result<Box<dyn Any + Send>, Error>>) -> Result<(), PipelineError> {
        if !results.is_empty() {
            return Err(PipelineError::TypeMismatch { index: 0 });
        }
        Ok(())
    }
}

/// Generate `UnfoldTuple` impls for nested type-level lists up to 8 elements.
///
/// Each invocation maps a nested type `(TN, (... (T0, ())))` to a flat
/// tuple `(Result<T0, Error>, ..., Result<TN, Error>)`.
macro_rules! impl_unfold_tuple {
    ($nested:ty, $count:literal, [$($idx:literal : $T:ident),+ $(,)?]) => {
        impl<$($T: Any + Send),+> UnfoldTuple for $nested {
            type Output = ($(Result<$T, Error>,)+);

            fn unfold(
                results: Vec<Result<Box<dyn Any + Send>, Error>>,
            ) -> Result<Self::Output, PipelineError> {
                if results.len() != $count {
                    return Err(PipelineError::TypeMismatch { index: 0 });
                }
                let mut iter = results.into_iter();
                Ok(($(
                    downcast_result::<$T>(
                        iter.next().ok_or(PipelineError::TypeMismatch { index: $idx })?,
                        $idx,
                    )?,
                )+))
            }
        }
    };
}

// 1 command: Accumulated = (A, ())
impl_unfold_tuple!((A, ()), 1, [0: A]);
// 2 commands: Accumulated = (B, (A, ()))
impl_unfold_tuple!((B, (A, ())), 2, [0: A, 1: B]);
// 3 commands: Accumulated = (C, (B, (A, ())))
impl_unfold_tuple!((C, (B, (A, ()))), 3, [0: A, 1: B, 2: C]);
// 4 commands
impl_unfold_tuple!((D, (C, (B, (A, ())))), 4, [0: A, 1: B, 2: C, 3: D]);
// 5 commands
impl_unfold_tuple!((E, (D, (C, (B, (A, ()))))), 5, [0: A, 1: B, 2: C, 3: D, 4: E]);
// 6 commands
impl_unfold_tuple!((F, (E, (D, (C, (B, (A, ())))))), 6, [0: A, 1: B, 2: C, 3: D, 4: E, 5: F]);
// 7 commands
impl_unfold_tuple!(
    (G, (F, (E, (D, (C, (B, (A, ()))))))),
    7,
    [0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G]
);
// 8 commands
impl_unfold_tuple!(
    (H, (G, (F, (E, (D, (C, (B, (A, ())))))))),
    8,
    [0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G, 7: H]
);

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

/// Typed pipeline builder for batching multiple pipelinable IMAP commands.
///
/// Built via [`ImapConnection::pipeline`]. Commands are added via
/// type-specific methods (e.g., [`fetch`](Self::fetch),
/// [`noop`](Self::noop)). Each method returns a new `Pipeline` with the
/// command's output type prepended to the `Accumulated` type-level list.
///
/// The accumulated type grows as a nested tuple:
/// `()` -> `(A, ())` -> `(B, (A, ()))` -> etc.
///
/// Call [`execute`](Self::execute) to submit the batch and receive a
/// flat, typed tuple of per-command results, or
/// [`execute_dynamic`](Self::execute_dynamic) for a `Vec<Result<...>>`.
///
/// # Example (illustrative  -  no wire activity)
///
/// ```text
/// let pipeline = conn.pipeline()
///     .noop()
///     .capability();
/// // pipeline: Pipeline<'_, (Vec<Capability>, ((), ()))>
/// // execute() would return (Result<(), Error>, Result<Vec<Capability>, Error>)
/// ```
pub struct Pipeline<'conn, Accumulated> {
    conn: &'conn super::ImapConnection,
    pending: Vec<Box<dyn ConsumerErased>>,
    commands: Vec<Command>,
    _marker: PhantomData<Accumulated>,
}

impl<'conn> Pipeline<'conn, ()> {
    /// Create a new empty pipeline for the given connection.
    pub(in crate::connection) fn new(conn: &'conn super::ImapConnection) -> Self {
        Self {
            conn,
            pending: Vec::new(),
            commands: Vec::new(),
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline command methods
// ---------------------------------------------------------------------------

/// Helper macro to generate pipeline command methods.
///
/// Each invocation adds a public method on `Pipeline<'conn, T>` that
/// pushes a `Command` variant and its consumer, returning a new pipeline
/// with the command's output type prepended to the accumulator.
macro_rules! pipeline_method {
    (
        $(#[$meta:meta])*
        fn $name:ident($($param:ident : $param_ty:ty),* $(,)?) -> $output:ty;
        command = $cmd:expr;
        consumer = $consumer:expr;
    ) => {
        $(#[$meta])*
        pub fn $name(mut self $(, $param: $param_ty)*) -> Pipeline<'conn, ($output, T)> {
            self.commands.push($cmd);
            self.pending.push(Box::new($consumer) as Box<dyn ConsumerErased>);
            Pipeline {
                conn: self.conn,
                pending: self.pending,
                commands: self.commands,
                _marker: PhantomData,
            }
        }
    };
}

impl<'conn, T> Pipeline<'conn, T> {
    // =======================================================================
    // Any-state commands (RFC 3501 Section6.1)
    // =======================================================================

    pipeline_method! {
        /// NOOP command (RFC 3501 Section6.1.2).
        fn noop() -> ();
        command = Command::Noop;
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// CAPABILITY command (RFC 3501 Section6.1.1).
        fn capability() -> Vec<Capability>;
        command = Command::Capability;
        consumer = dispatch::CapabilityConsumer::default();
    }

    // =======================================================================
    // Authenticated-state commands (RFC 3501 Section6.3)
    // =======================================================================

    pipeline_method! {
        /// LIST command (RFC 3501 Section6.3.8).
        fn list(reference: String, pattern: String) -> Result<Vec<MailboxInfo>, Error>;
        command = Command::List { reference, pattern };
        consumer = dispatch::ListConsumer::new();
    }

    /// LIST with selection/return options (RFC 5258 Section3, RFC 9051 Section6.3.9).
    ///
    /// The consumer's NOTIFY marker filter is derived from the
    /// `selection_options`: when `SUBSCRIBED` is absent, `\NonExistent`
    /// / `\NoAccess` responses are treated as NOTIFY markers and
    /// reclassified as events.
    pub fn list_extended(
        mut self,
        selection_options: Vec<String>,
        reference: String,
        patterns: Vec<String>,
        return_options: Vec<String>,
    ) -> Pipeline<'conn, (Result<Vec<MailboxInfo>, Error>, T)> {
        // RFC 5258 Section3: filter_extended is true when SUBSCRIBED is NOT
        // in the selection options  -  those responses are NOTIFY markers.
        let filter_extended = !selection_options
            .iter()
            .any(|o| o.eq_ignore_ascii_case("SUBSCRIBED"));
        let consumer =
            dispatch::ListExtendedConsumer::new(filter_extended, selection_options.clone());
        self.commands.push(Command::ListExtended {
            selection_options,
            reference,
            patterns,
            return_options,
        });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    pipeline_method! {
        /// LIST with STATUS return option (RFC 5819 Section2).
        fn list_status(
            reference: String,
            pattern: String,
            status_items: String,
        ) -> Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error>;
        command = Command::ListStatus { reference, pattern, status_items };
        consumer = dispatch::ListStatusConsumer::new();
    }

    pipeline_method! {
        /// LSUB command (RFC 3501 Section6.3.9).
        fn lsub(reference: String, pattern: String) -> Vec<MailboxInfo>;
        command = Command::Lsub { reference, pattern };
        consumer = dispatch::LsubConsumer::default();
    }

    pipeline_method! {
        /// CREATE command (RFC 3501 Section6.3.3).
        fn create(mailbox: MailboxName) -> Option<String>;
        command = Command::Create { mailbox };
        consumer = dispatch::CreateConsumer::default();
    }

    pipeline_method! {
        /// CREATE with USE special-use attributes (RFC 6154 Section3).
        fn create_special_use(
            mailbox: MailboxName,
            special_use: Vec<MailboxAttribute>,
        ) -> Option<String>;
        command = Command::CreateSpecialUse { mailbox, special_use };
        consumer = dispatch::CreateConsumer::default();
    }

    pipeline_method! {
        /// DELETE command (RFC 3501 Section6.3.4).
        fn delete(mailbox: MailboxName) -> ();
        command = Command::Delete { mailbox };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// RENAME command (RFC 3501 Section6.3.5).
        fn rename(mailbox: MailboxName, new_name: MailboxName) -> ();
        command = Command::Rename { mailbox, new_name };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// SUBSCRIBE command (RFC 3501 Section6.3.6).
        fn subscribe(mailbox: MailboxName) -> ();
        command = Command::Subscribe { mailbox };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// UNSUBSCRIBE command (RFC 3501 Section6.3.7).
        fn unsubscribe(mailbox: MailboxName) -> ();
        command = Command::Unsubscribe { mailbox };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// STATUS command (RFC 3501 Section6.3.10).
        fn status(mailbox: MailboxName, items: String) -> StatusResult;
        command = Command::Status { mailbox, items };
        consumer = dispatch::StatusConsumer::new();
    }

    pipeline_method! {
        /// NAMESPACE command (RFC 2342).
        fn namespace() -> NamespaceResponse;
        command = Command::Namespace;
        consumer = dispatch::NamespaceConsumer::default();
    }

    // =======================================================================
    // Selected-state commands (RFC 3501 Section6.4)
    // =======================================================================

    pipeline_method! {
        /// CHECK command (RFC 3501 Section6.4.1).
        fn check() -> ();
        command = Command::Check;
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// EXPUNGE command (RFC 3501 Section6.4.3).
        ///
        /// Pipelining EXPUNGE before sequence-number-based commands
        /// creates ambiguity because EXPUNGE renumbers messages
        /// (RFC 3501 Section5.5). Callers must ensure no such ambiguity
        /// exists in their pipeline.
        fn expunge() -> ExpungeResult;
        command = Command::Expunge;
        consumer = dispatch::ExpungeConsumer::new();
    }

    pipeline_method! {
        /// SEARCH command (RFC 3501 Section6.4.4).
        fn search(criteria: String) -> Result<SearchResult, Error>;
        command = Command::Search { criteria };
        consumer = dispatch::SearchConsumer::new();
    }

    pipeline_method! {
        /// SEARCH RETURN command (RFC 4731 Section3.2).
        fn search_return(
            criteria: String,
            return_opts: Vec<String>,
        ) -> Result<EsearchResponse, Error>;
        command = Command::SearchReturn { criteria, return_opts };
        consumer = dispatch::EsearchConsumer::new();
    }

    pipeline_method! {
        /// SEARCH RETURN (SAVE) command (RFC 5182 Section2).
        fn search_save(criteria: String) -> Result<(), Error>;
        command = Command::SearchSave { criteria };
        consumer = dispatch::SearchSaveConsumer::new();
    }

    pipeline_method! {
        /// FETCH command (RFC 3501 Section6.4.5).
        fn fetch(
            sequence_set: SequenceSet,
            items: String,
            changed_since: Option<u64>,
        ) -> Vec<FetchResponse>;
        command = Command::Fetch { sequence_set, items, changed_since };
        consumer = dispatch::FetchConsumer::new();
    }

    pipeline_method! {
        /// STORE command (RFC 3501 Section6.4.6).
        fn store(
            sequence_set: SequenceSet,
            operation: StoreOperation,
            flags: Vec<Flag>,
            unchanged_since: Option<u64>,
        ) -> StoreResult;
        command = Command::Store { sequence_set, operation, flags, unchanged_since };
        consumer = dispatch::StoreConsumer::new();
    }

    pipeline_method! {
        /// COPY command (RFC 3501 Section6.4.7).
        fn copy(sequence_set: SequenceSet, mailbox: MailboxName) -> CopyResult;
        command = Command::Copy { sequence_set, mailbox };
        consumer = dispatch::CopyConsumer::new();
    }

    pipeline_method! {
        /// MOVE command (RFC 6851 Section3).
        fn move_messages(sequence_set: SequenceSet, mailbox: MailboxName) -> MoveResult;
        command = Command::Move { sequence_set, mailbox };
        consumer = dispatch::MoveConsumer::new();
    }

    // =======================================================================
    // UID variants (RFC 3501 Section6.4.8)
    // =======================================================================

    pipeline_method! {
        /// UID SEARCH command (RFC 3501 Section6.4.4).
        fn uid_search(criteria: String) -> Result<SearchResult, Error>;
        command = Command::UidSearch { criteria };
        consumer = dispatch::SearchConsumer::new();
    }

    pipeline_method! {
        /// UID SEARCH RETURN command (RFC 4731 Section3.2).
        fn uid_search_return(
            criteria: String,
            return_opts: Vec<String>,
        ) -> Result<EsearchResponse, Error>;
        command = Command::UidSearchReturn { criteria, return_opts };
        consumer = dispatch::EsearchConsumer::new();
    }

    pipeline_method! {
        /// UID SEARCH RETURN (SAVE) command (RFC 5182 Section2).
        fn uid_search_save(criteria: String) -> Result<(), Error>;
        command = Command::UidSearchSave { criteria };
        consumer = dispatch::SearchSaveConsumer::new();
    }

    /// UID FETCH command (RFC 3501 Section6.4.5, RFC 7162 Section3.2.6).
    ///
    /// Uses [`FetchVanishedConsumer`](dispatch::FetchVanishedConsumer)
    /// regardless of the `vanished` flag so that VANISHED responses
    /// are always captured when the server sends them.
    ///
    /// Parses the `sequence_set` into a [`ParsedUidSet`] for defensive
    /// filtering of `VANISHED (EARLIER)` UIDs (RFC 7162 Section 3.2.6).
    #[allow(clippy::type_complexity)]
    pub fn uid_fetch(
        mut self,
        sequence_set: SequenceSet,
        items: String,
        changed_since: Option<u64>,
        vanished: bool,
    ) -> Pipeline<'conn, ((Vec<FetchResponse>, Vec<UidRange>), T)> {
        // Parse before moving sequence_set into the command.
        let parsed_set = ParsedUidSet::new(&sequence_set);
        self.commands.push(Command::UidFetch {
            sequence_set,
            items,
            changed_since,
            vanished,
        });
        self.pending
            .push(Box::new(dispatch::FetchVanishedConsumer::new(parsed_set))
                as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    pipeline_method! {
        /// UID STORE command (RFC 3501 Section6.4.6).
        fn uid_store(
            sequence_set: SequenceSet,
            operation: StoreOperation,
            flags: Vec<Flag>,
            unchanged_since: Option<u64>,
        ) -> StoreResult;
        command = Command::UidStore { sequence_set, operation, flags, unchanged_since };
        consumer = dispatch::StoreConsumer::new();
    }

    pipeline_method! {
        /// UID COPY command (RFC 3501 Section6.4.7).
        fn uid_copy(sequence_set: SequenceSet, mailbox: MailboxName) -> CopyResult;
        command = Command::UidCopy { sequence_set, mailbox };
        consumer = dispatch::CopyConsumer::new();
    }

    pipeline_method! {
        /// UID MOVE command (RFC 6851 Section3).
        fn uid_move_messages(sequence_set: SequenceSet, mailbox: MailboxName) -> MoveResult;
        command = Command::UidMove { sequence_set, mailbox };
        consumer = dispatch::MoveConsumer::new();
    }

    pipeline_method! {
        /// UID EXPUNGE command (RFC 4315).
        fn uid_expunge(sequence_set: SequenceSet) -> ExpungeResult;
        command = Command::UidExpunge { sequence_set };
        consumer = dispatch::ExpungeConsumer::new();
    }

    // =======================================================================
    // Extension commands
    // =======================================================================

    pipeline_method! {
        /// ID command (RFC 2971 Section3.1).
        fn id(params: Vec<(String, Option<String>)>) -> Vec<(String, Option<String>)>;
        command = Command::Id(params);
        consumer = dispatch::IdConsumer::default();
    }

    /// GETMETADATA command (RFC 5464 Section4.2).
    pub fn get_metadata(
        mut self,
        mailbox: MailboxName,
        entries: Vec<String>,
        max_size: Option<u64>,
        depth: Option<String>,
    ) -> Pipeline<'conn, (MetadataResult, T)> {
        let consumer = dispatch::MetadataConsumer::new(mailbox.as_str().to_owned());
        self.commands.push(Command::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    pipeline_method! {
        /// SETMETADATA command (RFC 5464 Section4.3).
        fn set_metadata(
            mailbox: MailboxName,
            entries: Vec<(String, Option<Vec<u8>>)>,
        ) -> ();
        command = Command::SetMetadata { mailbox, entries };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// THREAD command (RFC 5256 Section3).
        fn thread(algorithm: String, charset: String, criteria: String) -> Vec<ThreadNode>;
        command = Command::Thread { algorithm, charset, criteria };
        consumer = dispatch::ThreadConsumer::default();
    }

    pipeline_method! {
        /// UID THREAD command (RFC 5256 Section3).
        fn uid_thread(algorithm: String, charset: String, criteria: String) -> Vec<ThreadNode>;
        command = Command::UidThread { algorithm, charset, criteria };
        consumer = dispatch::ThreadConsumer::default();
    }

    pipeline_method! {
        /// SORT command (RFC 5256 Section2).
        fn sort(sort_criteria: String, charset: String, criteria: String) -> SearchResult;
        command = Command::Sort { sort_criteria, charset, criteria };
        consumer = dispatch::SortConsumer::default();
    }

    pipeline_method! {
        /// UID SORT command (RFC 5256 Section2).
        fn uid_sort(sort_criteria: String, charset: String, criteria: String) -> SearchResult;
        command = Command::UidSort { sort_criteria, charset, criteria };
        consumer = dispatch::SortConsumer::default();
    }

    pipeline_method! {
        /// NOTIFY SET command (RFC 5465 Section3).
        fn notify_set(params: NotifySetParams) -> Result<bool, Error>;
        command = Command::NotifySet(params);
        consumer = dispatch::NotifySetConsumer::default();
    }

    pipeline_method! {
        /// NOTIFY NONE command (RFC 5465 Section3).
        fn notify_none() -> ();
        command = Command::NotifyNone;
        consumer = dispatch::TaggedOkConsumer::default();
    }

    /// GETQUOTA command (RFC 2087 Section4.2).
    pub fn get_quota(mut self, root: String) -> Pipeline<'conn, (Vec<QuotaResource>, T)> {
        let consumer = dispatch::QuotaConsumer::new(root.clone());
        self.commands.push(Command::GetQuota { root });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    /// GETQUOTAROOT command (RFC 2087 Section4.3).
    pub fn get_quota_root(
        mut self,
        mailbox: MailboxName,
    ) -> Pipeline<'conn, (QuotaRootResponse, T)> {
        let consumer = dispatch::QuotaRootConsumer::new(mailbox.as_str().to_owned());
        self.commands.push(Command::GetQuotaRoot { mailbox });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    /// SETQUOTA command (RFC 2087 Section4.1).
    ///
    /// RFC 2087 Section4.1: the server MUST respond with QUOTA and QUOTAROOT
    /// untagged responses. The consumer captures the QUOTA response for
    /// the requested root.
    pub fn set_quota(
        mut self,
        root: String,
        resources: Vec<(String, u64)>,
    ) -> Pipeline<'conn, (Vec<QuotaResource>, T)> {
        let consumer = dispatch::QuotaConsumer::new(root.clone());
        self.commands.push(Command::SetQuota { root, resources });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    pipeline_method! {
        /// SETACL command (RFC 4314 Section3.1).
        fn set_acl(mailbox: MailboxName, identifier: String, rights: String) -> ();
        command = Command::SetAcl { mailbox, identifier, rights };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    pipeline_method! {
        /// DELETEACL command (RFC 4314 Section3.2).
        fn delete_acl(mailbox: MailboxName, identifier: String) -> ();
        command = Command::DeleteAcl { mailbox, identifier };
        consumer = dispatch::TaggedOkConsumer::default();
    }

    /// GETACL command (RFC 4314 Section3.3).
    pub fn get_acl(mut self, mailbox: MailboxName) -> Pipeline<'conn, (Vec<AclEntry>, T)> {
        let consumer = dispatch::AclConsumer::new(mailbox.as_str().to_owned());
        self.commands.push(Command::GetAcl { mailbox });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    /// LISTRIGHTS command (RFC 4314 Section3.4).
    pub fn list_rights(
        mut self,
        mailbox: MailboxName,
        identifier: String,
    ) -> Pipeline<'conn, (ListRightsResponse, T)> {
        let consumer =
            dispatch::ListRightsConsumer::new(mailbox.as_str().to_owned(), identifier.clone());
        self.commands.push(Command::ListRights {
            mailbox,
            identifier,
        });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    /// MYRIGHTS command (RFC 4314 Section3.5).
    pub fn my_rights(mut self, mailbox: MailboxName) -> Pipeline<'conn, (String, T)> {
        let consumer = dispatch::MyRightsConsumer::new(mailbox.as_str().to_owned());
        self.commands.push(Command::MyRights { mailbox });
        self.pending
            .push(Box::new(consumer) as Box<dyn ConsumerErased>);
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

impl<Accumulated: UnfoldTuple> Pipeline<'_, Accumulated> {
    /// Submit the accumulated commands as a single batch and await the
    /// typed result tuple.
    ///
    /// Each element in the returned tuple is `Result<T, Error>` where
    /// `T` is the command's consumer output type. Per-command errors
    /// (e.g., a server `NO` response) are captured per-element, not
    /// as a pipeline-level error.
    ///
    /// # Errors
    ///
    /// - [`PipelineError::Disconnected`]  -  the driver task has exited.
    /// - [`PipelineError::Driver`]  -  the driver aborted the entire
    ///   batch (e.g., encoding failure before any bytes were written).
    /// - [`PipelineError::TypeMismatch`]  -  internal bug in the
    ///   command-to-consumer mapping.
    pub async fn execute(self) -> Result<Accumulated::Output, PipelineError> {
        let results = self.execute_raw().await?;
        Accumulated::unfold(results)
    }
}

impl<T> Pipeline<'_, T> {
    /// Submit the accumulated commands and return the raw per-command
    /// results without typed unfolding.
    ///
    /// Each entry in the returned `Vec` corresponds to a command in
    /// push order. Callers must downcast `Box<dyn Any + Send>` to the
    /// expected consumer output type.
    ///
    /// Prefer [`execute`](Pipeline::execute) for type-safe access.
    pub async fn execute_dynamic(
        self,
    ) -> Result<Vec<Result<Box<dyn Any + Send>, Error>>, PipelineError> {
        self.execute_raw().await
    }

    /// Internal: send the pipeline to the driver and await the results.
    async fn execute_raw(self) -> Result<Vec<Result<Box<dyn Any + Send>, Error>>, PipelineError> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let dcmd = super::driver::DriverCommand::Pipeline {
            commands: self.commands,
            consumers: self.pending,
            result_tx,
        };
        if self.conn.cmd_tx.send(dcmd).await.is_err() {
            return Err(PipelineError::Disconnected);
        }
        match result_rx.await {
            Ok(Ok(results)) => Ok(results),
            Ok(Err(e)) => Err(PipelineError::Driver(e)),
            Err(_) => Err(PipelineError::Disconnected),
        }
    }
}

// ---------------------------------------------------------------------------
// ImapConnection::pipeline()
// ---------------------------------------------------------------------------

impl super::ImapConnection {
    /// Begin building a pipelined command batch (RFC 3501 Section5.5).
    ///
    /// Returns a [`Pipeline`] builder. Chain command methods to
    /// accumulate commands, then call [`Pipeline::execute`] to submit
    /// them all in one batch.
    ///
    /// Only commands that are safe to pipeline have methods on
    /// `Pipeline`. State-changing commands (SELECT, STARTTLS, IDLE,
    /// LOGOUT, AUTHENTICATE, etc.) are intentionally absent  -  the
    /// sealed `Pipelinable` trait enforces this at compile time.
    pub fn pipeline(&self) -> Pipeline<'_, ()> {
        Pipeline::new(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
