//! Account-oriented multi-recipient send.
//!
//! 1. Validate `BatchItem<Address>` input as `RequestErrorKind::BatchInputInvalid`
//!    before any wire activity.
//! 2. Provide a `SendProgress` tracker that captures the per-recipient command
//!    sequence (MAIL FROM, RCPT TO, DATA/BDAT command, body start, final
//!    reply) and resolves into `BatchOutcome<()>` lanes. The tracker is the
//!    single source of truth for SMTP and LMTP lane assignment; both
//!    sequential and pipelined paths drive it the same way, and the
//!    post-DATA partial-completion case becomes `Uncertain` with
//!    `Reconcile(PartialCompletionSignal)` recovery.
//!
//! The existing transport `send_raw*` paths are unchanged. The new batch
//! helpers on `SmtpTransport` / `LmtpTransport` build a `SendProgress`, drive
//! it through the existing command sequence, then resolve into a
//! `BatchOutcome<()>`.

use bifrost_types::error::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AttemptCause,
    BatchInputInvalidItem, BatchItem, BatchItemId, BatchOutcome, Cause, DiagnosticText, Protocol,
    ProtocolErrorKind, RequestCause, RequestErrorKind, TransmissionState, WireCause,
};

use crate::address::Address;
use crate::transport::smtp::account_error::{
    SmtpErrorContext, into_account_error, response_to_account_error,
};
use crate::transport::smtp::error::{Error as SmtpError, SmtpCommandPhase, SmtpTransmissionState};
use crate::transport::smtp::response::Response;

/// Account-oriented recipient input. The address part is the SMTP envelope
/// recipient; the id is opaque to SMTP and round-tripped to each lane entry.
#[derive(Clone, Debug)]
pub(crate) struct SmtpBatchRecipient {
    pub id: BatchItemId,
    pub address: Address,
}

impl From<BatchItem<Address>> for SmtpBatchRecipient {
    fn from(item: BatchItem<Address>) -> Self {
        Self {
            id: item.id,
            address: item.input,
        }
    }
}

/// Per-recipient state in the send-command sequence.
#[derive(Clone, Debug)]
pub(crate) enum RcptProgress {
    /// RCPT TO has not been issued yet.
    Pending,
    /// RCPT TO returned 2xx.
    Accepted,
    /// RCPT TO returned 4xx/5xx. Will become a `failed` lane.
    Rejected(Response),
    /// LMTP post-DATA final status returned for this recipient (any class).
    Final(Response),
    /// Transport drop / connection failure happened while the recipient's
    /// outcome was still ambiguous. Will become an `uncertain` lane.
    Uncertain(AccountError),
    /// Transport failure happened before any message content could have
    /// reached the peer (the transaction died in the envelope phase, with
    /// `DATA` never issued). Will become a `failed` lane carrying `Unsent`
    /// evidence, so the caller may safely retry the whole recipient.
    Unsent(AccountError),
}

#[derive(Clone, Debug)]
pub(crate) struct RecipientProgress {
    pub(crate) id: BatchItemId,
    pub(crate) address: Address,
    pub(crate) rcpt: RcptProgress,
}

/// Drives the entire batch transaction. SMTP and LMTP both build one of these
/// and call `resolve()` at the end.
#[derive(Clone, Debug)]
pub(crate) struct SendProgress {
    pub(crate) protocol: Protocol,
    pub(crate) recipients: Vec<RecipientProgress>,
    pub(crate) body_started: bool,
    pub(crate) body_finished: bool,
    /// SMTP-only: the final DATA/BDAT reply applies to all accepted
    /// recipients. None for LMTP (which has per-recipient final replies).
    pub(crate) data_response: Option<Response>,
}

impl SendProgress {
    pub(crate) fn new(protocol: Protocol, recipients: Vec<SmtpBatchRecipient>) -> Self {
        Self {
            protocol,
            recipients: recipients
                .into_iter()
                .map(|r| RecipientProgress {
                    id: r.id,
                    address: r.address,
                    rcpt: RcptProgress::Pending,
                })
                .collect(),
            body_started: false,
            body_finished: false,
            data_response: None,
        }
    }

    pub(crate) fn record_rcpt_accepted(&mut self, index: usize) {
        if let Some(rec) = self.recipients.get_mut(index) {
            rec.rcpt = RcptProgress::Accepted;
        }
    }

    pub(crate) fn record_rcpt_rejected(&mut self, index: usize, response: Response) {
        if let Some(rec) = self.recipients.get_mut(index) {
            rec.rcpt = RcptProgress::Rejected(response);
        }
    }

    pub(crate) fn record_lmtp_final(&mut self, index: usize, response: Response) {
        if let Some(rec) = self.recipients.get_mut(index) {
            rec.rcpt = RcptProgress::Final(response);
        }
    }

    pub(crate) fn mark_uncertain_unresolved(&mut self, error_factory: impl Fn() -> AccountError) {
        for rec in &mut self.recipients {
            if matches!(rec.rcpt, RcptProgress::Accepted | RcptProgress::Pending) {
                rec.rcpt = RcptProgress::Uncertain(error_factory());
            }
        }
    }

    /// Mark every not-yet-decided recipient as `Unsent`, preserving RCPT
    /// replies the peer already gave.
    ///
    /// Used for envelope-phase transport failures: `DATA` has not been issued,
    /// so no message content can have reached the peer and `Uncertain` would
    /// be false evidence. Recipients the server already accepted or rejected
    /// keep that answer; everything still open becomes a retryable failure.
    pub(crate) fn mark_unresolved_unsent(&mut self, error_factory: impl Fn() -> AccountError) {
        for rec in &mut self.recipients {
            if matches!(rec.rcpt, RcptProgress::Accepted | RcptProgress::Pending) {
                rec.rcpt = RcptProgress::Unsent(error_factory());
            }
        }
    }

    /// Mark all `Accepted` recipients as `Rejected` with the given DATA-level
    /// response. Used when the DATA command reply is negative before the body
    /// was started.
    pub(crate) fn mark_accepted_rejected_with_response(&mut self, response: Response) {
        for rec in &mut self.recipients {
            if matches!(rec.rcpt, RcptProgress::Accepted) {
                rec.rcpt = RcptProgress::Rejected(response.clone());
            }
        }
    }

    /// Mark all `Accepted` recipients as `Uncertain` with the given error.
    ///
    /// Used when an error leaves accepted recipients in an ambiguous state
    /// (e.g. transport drop after DATA body was sent, or DATA command transport
    /// error where we cannot attribute the failure per-recipient).
    pub(crate) fn mark_accepted_uncertain(&mut self, error_factory: impl Fn() -> AccountError) {
        for rec in &mut self.recipients {
            if matches!(rec.rcpt, RcptProgress::Accepted) {
                rec.rcpt = RcptProgress::Uncertain(error_factory());
            }
        }
    }

    pub(crate) fn set_body_started(&mut self) {
        self.body_started = true;
    }

    pub(crate) fn set_body_finished(&mut self) {
        self.body_finished = true;
    }

    pub(crate) fn set_data_response(&mut self, response: Response) {
        self.data_response = Some(response);
    }

    /// Build the `BatchOutcome<()>` from the recorded progress. Lane order
    /// follows input order.
    pub(crate) fn resolve(self) -> BatchOutcome<()> {
        let mut builder = bifrost_types::BatchOutcomeBuilder::<()>::new();
        let protocol = self.protocol;
        let smtp_final = self.data_response.clone();
        let ids: Vec<_> = self.recipients.iter().map(|r| r.id.clone()).collect();
        for rec in self.recipients {
            match rec.rcpt {
                RcptProgress::Accepted => {
                    // SMTP: all accepted recipients share the DATA-final
                    // outcome. LMTP: per-recipient final reply is required;
                    // an Accepted-without-Final at resolve time is a
                    // programming bug (the LMTP final-status drain should
                    // have transitioned every Accepted to Final or
                    // Uncertain). debug_assert! catches the bug in tests
                    // and the defensive partial-completion fallback keeps
                    // the lane from silently disappearing in release.
                    if let Some(resp) = &smtp_final {
                        if resp.is_positive() {
                            builder.push_succeeded(rec.id, ());
                        } else {
                            // Negative final reply after a successful body
                            // upload: classify under `DataFinal`, not
                            // `DataBody`. A future phase-aware rule can
                            // then distinguish a body-side transport drop
                            // from a server-rejected final reply.
                            let err = response_to_account_error(
                                resp,
                                &SmtpErrorContext::send(protocol)
                                    .with_phase(SmtpCommandPhase::DataFinal),
                                Some(SmtpCommandPhase::DataFinal),
                                Some(SmtpTransmissionState::Acknowledged),
                            );
                            builder.push_failed(rec.id, with_recipient_text(err, &rec.address));
                        }
                    } else {
                        debug_assert!(
                            protocol == Protocol::Smtp,
                            "LMTP Accepted without Final at resolve time: missing \
                             per-recipient transition in the final-status drain"
                        );
                        let err = partial_completion_error(protocol, &rec.address);
                        builder.push_uncertain(rec.id, with_recipient_text(err, &rec.address));
                    }
                }
                RcptProgress::Rejected(resp) => {
                    let err = response_to_account_error(
                        &resp,
                        &SmtpErrorContext::send(protocol).with_phase(SmtpCommandPhase::RcptTo),
                        Some(SmtpCommandPhase::RcptTo),
                        Some(SmtpTransmissionState::Acknowledged),
                    );
                    builder.push_failed(rec.id, with_recipient_text(err, &rec.address));
                }
                RcptProgress::Final(resp) => {
                    if resp.is_positive() {
                        builder.push_succeeded(rec.id, ());
                    } else {
                        let err = response_to_account_error(
                            &resp,
                            &SmtpErrorContext::send(protocol)
                                .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                            Some(SmtpCommandPhase::LmtpFinalStatus),
                            Some(SmtpTransmissionState::Acknowledged),
                        );
                        builder.push_failed(rec.id, with_recipient_text(err, &rec.address));
                    }
                }
                RcptProgress::Uncertain(err) => {
                    builder.push_uncertain(rec.id, with_recipient_text(err, &rec.address));
                }
                RcptProgress::Unsent(err) => {
                    // Nothing was transmitted for this recipient, so this is a
                    // plain failure with `Unsent` evidence - not an uncertain
                    // lane the caller has to reconcile by hand.
                    builder.push_failed(rec.id, with_recipient_text(err, &rec.address));
                }
                RcptProgress::Pending => {
                    // Pending at resolve time means the drain never reached
                    // this recipient. Surface as uncertain so no item is
                    // silently dropped.
                    builder.push_uncertain(
                        rec.id,
                        with_recipient_text(
                            partial_completion_error(protocol, &rec.address),
                            &rec.address,
                        ),
                    );
                }
            }
        }
        builder
            .finalize(&ids)
            .expect("smtp resolve produces exactly one outcome per submitted recipient")
    }
}

fn with_recipient_text(error: AccountError, address: &Address) -> AccountError {
    // Per-recipient correlation: the DATA-final-negative fanout to N
    // accepted recipients shares the same wire response text, and the
    // RcptTo-rejection text may or may not embed the address. Attach the
    // address explicitly as a support-only diagnostic so support exports
    // can correlate a lane back to its envelope recipient without parsing
    // the response text.
    error
        .into_builder()
        .text(DiagnosticText::support_only(format!(
            "envelope recipient {address}"
        )))
        .try_build()
        .expect("valid account error classification")
}

fn partial_completion_error(protocol: Protocol, address: &Address) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol,
            detail: Some(DiagnosticText::support_only(format!(
                "transport drop after body write; recipient {address} uncertain"
            ))),
        }),
    )
    .protocol(protocol)
    .operation(AccountOperation::Send)
    .idempotency_override(false)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .text(DiagnosticText::support_only(format!(
        "recipient {address} uncertain after body write"
    )))
    .try_build()
    .expect("valid account error classification")
}

pub(crate) fn batch_input_invalid_error(
    protocol: Protocol,
    items: Vec<BatchInputInvalidItem>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid),
        Cause::Request(RequestCause::BatchInputInvalid { items }),
    )
    .protocol(protocol)
    .operation(AccountOperation::Send)
    .idempotency_override(false)
    .try_build()
    .expect("valid account error classification")
}

/// Convert a fatal transport-level `SmtpError` that aborted the batch into the
/// `Err(AccountError)` returned by the batch helpers. This is the path used
/// when no recipient-specific outcome can be reported (MAIL FROM reject,
/// pre-MAIL transport drop, AUTH failure, etc.).
pub(crate) fn batch_level_error(error: SmtpError, ctx: SmtpErrorContext) -> AccountError {
    into_account_error(error, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::smtp::response::{Category, Code, Detail, Severity};
    use bifrost_types::error::{
        AccountErrorKind, BatchInputInvalidReason, BatchItem, BatchItemId, ReconcileReason,
        RecoveryClass, RequestErrorKind, ResourceKind, validate_batch_input,
    };

    fn recip(id: &str, addr: &str) -> SmtpBatchRecipient {
        SmtpBatchRecipient {
            id: BatchItemId(id.to_owned()),
            address: addr.parse().expect("valid address"),
        }
    }

    fn positive_data_final() -> Response {
        Response::new(
            Code::new(
                Severity::PositiveCompletion,
                Category::MailSystem,
                Detail::Zero,
            ),
            vec!["2.0.0 queued".to_owned()],
        )
    }

    fn negative_data_final() -> Response {
        Response::new(
            Code::new(
                Severity::PermanentNegativeCompletion,
                Category::MailSystem,
                Detail::Four,
            ),
            vec!["5.5.4 transaction failed".to_owned()],
        )
    }

    fn rcpt_reject() -> Response {
        Response::new(
            Code::new(
                Severity::PermanentNegativeCompletion,
                Category::Information,
                Detail::One,
            ),
            vec!["5.1.1 user unknown".to_owned()],
        )
    }

    #[test]
    fn validation_rejects_empty() {
        let items: Vec<BatchItem<Address>> = Vec::new();
        let err = validate_batch_input(&items).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].reason, BatchInputInvalidReason::Empty);
        let account = batch_input_invalid_error(Protocol::Smtp, err);
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid)
        ));
    }

    #[test]
    fn validation_rejects_duplicate_ids() {
        let items = vec![
            BatchItem::new(
                BatchItemId("a".to_owned()),
                "a@example.com".parse::<Address>().unwrap(),
            ),
            BatchItem::new(
                BatchItemId("a".to_owned()),
                "b@example.com".parse::<Address>().unwrap(),
            ),
        ];
        let err = validate_batch_input(&items).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].reason, BatchInputInvalidReason::Duplicate);
    }

    #[test]
    fn smtp_all_succeed() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![recip("a", "a@x.com"), recip("b", "b@x.com")],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        progress.set_body_started();
        progress.set_body_finished();
        progress.set_data_response(positive_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.succeeded().len(), 2);
        assert_eq!(outcome.failed().len(), 0);
        assert_eq!(outcome.uncertain().len(), 0);
    }

    #[test]
    fn smtp_mixed_rcpt_rejects_split_lanes() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![
                recip("a", "good@x.com"),
                recip("b", "bad@x.com"),
                recip("c", "ok@x.com"),
            ],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_rejected(1, rcpt_reject());
        progress.record_rcpt_accepted(2);
        progress.set_body_started();
        progress.set_body_finished();
        progress.set_data_response(positive_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.succeeded().len(), 2);
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "b");
        assert!(matches!(
            outcome.failed()[0].error.kind(),
            AccountErrorKind::NotFound(ResourceKind::Mailbox)
        ));
    }

    #[test]
    fn smtp_all_rcpt_rejected_is_ok_batch_outcome() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![recip("a", "x@x.com"), recip("b", "y@x.com")],
        );
        progress.record_rcpt_rejected(0, rcpt_reject());
        progress.record_rcpt_rejected(1, rcpt_reject());
        // No DATA/body. Resolve must not panic and must produce 2 failures.
        let outcome = progress.resolve();
        assert_eq!(outcome.failed().len(), 2);
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.uncertain().len(), 0);
    }

    #[test]
    fn smtp_data_negative_marks_accepted_failed() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![recip("a", "x@x.com"), recip("b", "y@x.com")],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        progress.set_body_started();
        progress.set_body_finished();
        progress.set_data_response(negative_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.failed().len(), 2);
        assert_eq!(outcome.succeeded().len(), 0);
    }

    #[test]
    fn smtp_drop_after_body_marks_accepted_uncertain() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![recip("a", "x@x.com"), recip("b", "y@x.com")],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        progress.set_body_started();
        // No data_response: connection dropped after body write.
        let outcome = progress.resolve();
        assert_eq!(outcome.uncertain().len(), 2);
        // Recovery is Reconcile(PartialCompletionSignal) because non-idempotent Send.
        let r = outcome.uncertain()[0].error.recovery();
        assert!(matches!(
            r,
            RecoveryClass::Reconcile(advice) if matches!(advice.reason, ReconcileReason::PartialCompletionSignal)
        ));
    }

    #[test]
    fn pipelining_rcpt_failure_is_per_recipient_lane() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![recip("a", "x@x.com"), recip("b", "y@x.com")],
        );
        // Pipelined drain: first reply rejects, second accepts.
        progress.record_rcpt_rejected(0, rcpt_reject());
        progress.record_rcpt_accepted(1);
        progress.set_body_started();
        progress.set_body_finished();
        progress.set_data_response(positive_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.succeeded().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "a");
        assert_eq!(outcome.succeeded()[0].item.0, "b");
    }

    #[test]
    fn pipelining_drain_drop_marks_unresolved_uncertain() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![
                recip("a", "x@x.com"),
                recip("b", "y@x.com"),
                recip("c", "z@x.com"),
            ],
        );
        // First RCPT replied accepted before drain dropped.
        progress.record_rcpt_accepted(0);
        // Recipients b and c are still Pending. Simulate batch tracker marking
        // unresolved uncertain on a transport drop during drain.
        progress.mark_uncertain_unresolved(|| {
            partial_completion_error(Protocol::Smtp, &"y@x.com".parse::<Address>().unwrap())
        });

        let outcome = progress.resolve();
        assert_eq!(outcome.uncertain().len(), 3);
    }

    #[test]
    fn envelope_phase_write_failure_marks_unresolved_unsent_and_keeps_rejections() {
        let mut progress = SendProgress::new(
            Protocol::Smtp,
            vec![
                recip("a", "x@x.com"),
                recip("b", "y@x.com"),
                recip("c", "z@x.com"),
            ],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_rejected(1, rcpt_reject());
        // c never got its RCPT written: a later PIPELINING window failed to
        // write, and DATA was never issued.
        progress.mark_unresolved_unsent(|| {
            partial_completion_error(Protocol::Smtp, &"z@x.com".parse::<Address>().unwrap())
        });

        let outcome = progress.resolve();
        assert!(
            outcome.uncertain().is_empty(),
            "no content reached the peer, so nothing may claim uncertain evidence"
        );
        assert_eq!(outcome.failed().len(), 3);
        assert_eq!(outcome.succeeded().len(), 0);
        // The peer's own RCPT rejection survives the transport failure.
        assert_eq!(outcome.failed()[1].item.0, "b");
    }

    #[test]
    fn lmtp_mixed_final_statuses_preserve_order() {
        let mut progress = SendProgress::new(
            Protocol::Lmtp,
            vec![
                recip("a", "x@x.com"),
                recip("b", "y@x.com"),
                recip("c", "z@x.com"),
            ],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        progress.record_rcpt_accepted(2);
        progress.set_body_started();
        progress.set_body_finished();
        progress.record_lmtp_final(0, positive_data_final());
        progress.record_lmtp_final(1, negative_data_final());
        progress.record_lmtp_final(2, positive_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.succeeded().len(), 2);
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "b");
        let lane_order: Vec<_> = outcome
            .iter()
            .map(|item| match item {
                bifrost_types::error::BatchItemOutcome::Succeeded(s) => s.item.0.as_str(),
                bifrost_types::error::BatchItemOutcome::Failed(f) => f.item.0.as_str(),
                bifrost_types::error::BatchItemOutcome::Uncertain(u) => u.item.0.as_str(),
            })
            .collect();
        assert_eq!(lane_order, vec!["a", "b", "c"]);
    }

    #[test]
    fn lmtp_final_read_drop_keeps_read_marks_rest_uncertain() {
        let mut progress = SendProgress::new(
            Protocol::Lmtp,
            vec![
                recip("a", "x@x.com"),
                recip("b", "y@x.com"),
                recip("c", "z@x.com"),
            ],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        progress.record_rcpt_accepted(2);
        progress.set_body_started();
        progress.record_lmtp_final(0, positive_data_final());
        // b and c never got their final status.
        progress.mark_uncertain_unresolved(|| {
            partial_completion_error(Protocol::Lmtp, &"y@x.com".parse::<Address>().unwrap())
        });

        let outcome = progress.resolve();
        assert_eq!(outcome.succeeded().len(), 1);
        assert_eq!(outcome.uncertain().len(), 2);
        assert_eq!(outcome.succeeded()[0].item.0, "a");
    }

    #[test]
    fn bdat_unsupported_maps_to_unsupported_send_via_wire() {
        // Synthetic 502 (command not implemented) maps to Unsupported(Send) via
        // the status table. Validate that the response-classifier returns the
        // right kind when used for a BDAT command failure.
        let resp = Response::new(
            Code::new(
                Severity::PermanentNegativeCompletion,
                Category::Syntax,
                Detail::Two,
            ),
            vec!["502 command not implemented".to_owned()],
        );
        let err = response_to_account_error(
            &resp,
            &SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::DataCommand),
            Some(SmtpCommandPhase::DataCommand),
            Some(SmtpTransmissionState::Acknowledged),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn smtp_data_final_negative_uses_data_final_phase() {
        // DATA-final-negative reply (the server reads the body, then rejects)
        // must classify under SmtpCommandPhase::DataFinal, not DataBody. A
        // body-side phase tag would conflate this with a transport drop
        // mid-body in any future phase-aware classifier.
        let mut progress = SendProgress::new(Protocol::Smtp, vec![recip("a", "a@x.com")]);
        progress.record_rcpt_accepted(0);
        progress.set_body_started();
        progress.set_body_finished();
        progress.set_data_response(negative_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.failed().len(), 1);
        // The error must be acknowledged (server replied) and carry a
        // request scope that surfaces the recipient. The dotted enhanced
        // status (5.5.4 transaction failed) is class-5 subject-5 detail-4
        // which classifies as Request(Malformed) per the enhanced table -
        // pin the kind so a regression in classify_enhanced cannot pretend
        // DataFinal worked while changing the routed kind.
        let err = &outcome.failed()[0].error;
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        // Recipient correlation must reach support exports.
        let support = err.support_consented();
        let texts = &support.support_text;
        assert!(
            texts.iter().any(|t| t.contains("envelope recipient")),
            "expected per-recipient diagnostic in support text, got {texts:?}"
        );
    }

    #[test]
    fn lmtp_accepted_without_final_at_resolve_is_uncertain_in_release() {
        // Defensive fallback: if the LMTP final-status drain failed to
        // transition an Accepted recipient to Final, resolve() must not
        // silently drop the lane. In debug builds the debug_assert!
        // catches the programming bug. We exercise the fallback shape
        // here directly by skipping the final-status drain.
        //
        // The test is only meaningful in release builds (debug_assert!
        // would panic). Skip when debug_assertions is on so the regression
        // test of the fallback path stays exercised on release CI without
        // breaking debug-build CI.
        if cfg!(debug_assertions) {
            return;
        }
        let mut progress = SendProgress::new(Protocol::Lmtp, vec![recip("a", "a@x.com")]);
        progress.record_rcpt_accepted(0);
        progress.set_body_started();
        // No record_lmtp_final, no mark_uncertain_unresolved: bug path.
        let outcome = progress.resolve();
        assert_eq!(outcome.uncertain().len(), 1);
    }

    #[test]
    fn lmtp_data_command_negative_reply_marks_recipients_failed() {
        // smtp-D1 / P0 regression: LMTP DATA-command negative reply after
        // RCPT acceptances must produce per-recipient `Failed` lanes and a
        // succeeded outcome at the batch boundary, NEVER a batch-level
        // `Err`. The previous shape returned `Err((Network+Unsent,
        // progress))` after RCPT acceptances; the engine then classified
        // `Send` as `Retry::SameRequest` and resent the entire non-
        // idempotent message to every recipient after the server already
        // rejected it.
        let mut progress = SendProgress::new(
            Protocol::Lmtp,
            vec![recip("a", "a@x.com"), recip("b", "b@x.com")],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        // Now the LMTP DATA command returns a negative reply; this is the
        // shape `mark_accepted_rejected_with_response` produces. No body
        // was sent; no LMTP final-status drain ran.
        progress.mark_accepted_rejected_with_response(negative_data_final());

        let outcome = progress.resolve();
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.uncertain().len(), 0);
        assert_eq!(outcome.failed().len(), 2);
        // The recovery class must be terminal-or-retry, never Reconcile
        // (Reconcile would indicate non-idempotent uncertain). Since the
        // server explicitly rejected, recovery must be acknowledged.
        for failed in outcome.failed() {
            let recovery = failed.error.recovery();
            assert!(
                !recovery.requires_reconciliation(),
                "LMTP DATA-command negative reply must not produce a Reconcile lane: {recovery:?}"
            );
        }
    }

    #[test]
    fn lmtp_data_command_transport_drop_marks_recipients_uncertain_inflight() {
        // smtp-D1: transport drop on the LMTP DATA-command write/read uses
        // `InFlight` + `DataCommand`. Accepted recipients become
        // `Uncertain` with recovery `Reconcile(TransportDropAfterSend)`
        // because Send is non-idempotent.
        use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
        use crate::transport::smtp::error::{Error as SmtpError, ErrorKind};
        let mut progress = SendProgress::new(
            Protocol::Lmtp,
            vec![recip("a", "a@x.com"), recip("b", "b@x.com")],
        );
        progress.record_rcpt_accepted(0);
        progress.record_rcpt_accepted(1);
        // Simulate the call site building the AccountError for a
        // DataCommand-phase transport drop.
        let smtp_err = SmtpError::new(
            ErrorKind::Network,
            Some(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset",
            )),
        )
        .with_attempt(SmtpTransmissionState::InFlight)
        .with_phase(SmtpCommandPhase::DataCommand);
        let ae = into_account_error(
            smtp_err,
            SmtpErrorContext::send(Protocol::Lmtp).with_phase(SmtpCommandPhase::DataCommand),
        );
        let ae2 = ae.clone();
        progress.mark_accepted_uncertain(|| ae2.clone());

        let outcome = progress.resolve();
        assert_eq!(outcome.uncertain().len(), 2);
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.failed().len(), 0);
        for unc in outcome.uncertain() {
            let recovery = unc.error.recovery();
            assert!(
                matches!(
                    recovery,
                    RecoveryClass::Reconcile(advice)
                        if matches!(advice.reason, ReconcileReason::TransportDropAfterSend)
                ),
                "LMTP DataCommand transport drop must be Reconcile(TransportDropAfterSend): {recovery:?}"
            );
        }
    }

    #[test]
    fn rcpt_failed_lane_carries_recipient_text() {
        // Per-recipient correlation requirement: failed lanes from a RCPT-
        // rejected reply must surface the envelope recipient as
        // support-only diagnostic text. Sharing only the wire response
        // text loses correlation when a DATA-final-negative fanout
        // produces N lanes with identical response text.
        let mut progress = SendProgress::new(Protocol::Smtp, vec![recip("a", "a@x.com")]);
        progress.record_rcpt_rejected(0, rcpt_reject());
        let outcome = progress.resolve();
        let err = &outcome.failed()[0].error;
        let support = err.support_consented();
        let texts = &support.support_text;
        assert!(
            texts.iter().any(|t| t.contains("a@x.com")),
            "expected recipient address in support text, got {texts:?}"
        );
    }
}
