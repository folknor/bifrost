//! Protocol rules pinned once, against the core, with no I/O.
//!
//! The transcript suites in both drivers still drive the full wire exchange
//! through their adapter; these tests pin the rules the adapters no longer
//! own, in the one place they now live. A machine is driven by a scripted
//! sequence of outcomes and its op stream is recorded, so "which command, how
//! many reply reads, RSET or abort, which phase" are all directly assertable.

use bifrost_types::error::BatchItemId;

use super::{super::PIPELINING_RECIPIENT_WINDOW, *};
use crate::{
    address::Address,
    transport::smtp::{
        batch::SmtpBatchRecipient,
        commands::{Mail, Rcpt},
        error::ErrorKind,
        response::{Category, Code, Detail, Response, Severity},
    },
};

fn address(local: &str) -> Address {
    format!("{local}@example.com")
        .parse()
        .expect("valid address")
}

fn mail() -> Mail {
    Mail::new(Some(address("sender")), Vec::new()).expect("valid MAIL FROM")
}

fn rcpts(count: usize) -> Vec<Rcpt> {
    (0..count)
        .map(|index| Rcpt::new(address(&format!("rcpt-{index}")), Vec::new()).expect("valid RCPT"))
        .collect()
}

fn reply(severity: Severity, message: &str) -> Response {
    Response::new(
        Code::new(severity, Category::MailSystem, Detail::Zero),
        vec![message.to_owned()],
    )
}

fn ok_reply() -> Response {
    reply(Severity::PositiveCompletion, "ok")
}

fn rejection() -> Response {
    reply(Severity::PermanentNegativeCompletion, "rejected")
}

fn intermediate() -> Response {
    reply(Severity::PositiveIntermediate, "send body")
}

/// A recording driver: performs no I/O, answers each op from a script, and
/// keeps the op stream for assertions.
struct Harness {
    script: Vec<OpOutcome>,
    ops: Vec<String>,
}

impl Harness {
    fn new(script: Vec<OpOutcome>) -> Self {
        Self {
            script,
            ops: Vec::new(),
        }
    }

    /// A script of positive replies long enough that reads never run out.
    fn positive(len: usize) -> Self {
        Self::new((0..len).map(|_| OpOutcome::Reply(ok_reply())).collect())
    }

    fn run<M: ProtocolMachine>(&mut self, machine: &mut M) -> M::Output {
        let mut outcome = OpOutcome::Done;
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 512, "machine did not terminate: {:?}", self.ops);
            match machine.step(outcome) {
                Step::Finish(output) => return output,
                Step::Run(op) => {
                    self.ops.push(render(&op));
                    outcome = match op {
                        // Writes and group boundaries answer `Done` unless the
                        // script says otherwise; reads always consume a step.
                        Op::ReadGrouped | Op::ReadSingle => {
                            self.next().unwrap_or_else(|| OpOutcome::Reply(ok_reply()))
                        }
                        _ => self.next_non_reply(),
                    };
                }
            }
        }
    }

    fn next(&mut self) -> Option<OpOutcome> {
        if self.script.is_empty() {
            return None;
        }
        Some(self.script.remove(0))
    }

    /// Writes and group boundaries only consume a scripted outcome when it is
    /// a failure, so a script of replies stays aligned with the reads.
    fn next_non_reply(&mut self) -> OpOutcome {
        if matches!(self.script.first(), Some(OpOutcome::Failed(_))) {
            self.script.remove(0)
        } else {
            OpOutcome::Done
        }
    }
}

fn render(op: &Op) -> String {
    match op {
        Op::Write(bytes) => format!("W:{}", bytes.replace("\r\n", "|")),
        Op::WriteBody => "BODY".to_owned(),
        Op::WriteBdat => "BDAT".to_owned(),
        Op::OpenReplyGroup => "OPEN".to_owned(),
        Op::ReadGrouped => "READG".to_owned(),
        Op::ReadSingle => "READ".to_owned(),
        Op::CloseReplyGroup => "CLOSE".to_owned(),
        Op::CloseLmtpDrain { restore_ok } => format!("DRAIN:{restore_ok}"),
        Op::Abort => "ABORT".to_owned(),
    }
}

fn network_failure() -> OpOutcome {
    OpOutcome::Failed(crate::transport::smtp::error::network(
        std::io::Error::other("peer went away"),
    ))
}

fn batch_recipients(count: usize) -> Vec<SmtpBatchRecipient> {
    (0..count)
        .map(|index| SmtpBatchRecipient {
            id: BatchItemId(format!("item-{index}")),
            address: address(&format!("rcpt-{index}")),
        })
        .collect()
}

/// One window's worth of RCPT commands go out in a single write, and exactly
/// one reply per command in that window is drained before the group closes.
/// Getting the count wrong is how a driver reads the next window's replies as
/// this window's.
#[test]
fn a_pipelined_window_drains_exactly_one_reply_per_command() {
    let count = PIPELINING_RECIPIENT_WINDOW + 3;
    let mut machine = DirectSmtp::new(mail(), rcpts(count), true, BodyKind::Data);
    let mut harness = Harness::positive(count + 8);
    harness.run(&mut machine).expect("the send succeeds");

    let first_window = harness.ops.first().expect("a window is written first");
    assert_eq!(
        first_window.matches("RCPT TO").count(),
        PIPELINING_RECIPIENT_WINDOW,
        "the first window is bounded: {:?}",
        harness.ops
    );
    assert!(
        first_window.starts_with("W:MAIL FROM"),
        "MAIL FROM shares the first window's write: {first_window}"
    );

    // Window 0: MAIL FROM's reply plus one per recipient, then the close.
    let reads_before_first_close = harness
        .ops
        .iter()
        .take_while(|op| *op != "CLOSE")
        .filter(|op| *op == "READG")
        .count();
    assert_eq!(
        reads_before_first_close,
        PIPELINING_RECIPIENT_WINDOW + 1,
        "one reply per command in the window, plus MAIL FROM: {:?}",
        harness.ops
    );

    // Window 1 carries only the remaining recipients.
    let second_window = harness
        .ops
        .iter()
        .rfind(|op| op.starts_with("W:RCPT"))
        .expect("a second window is written");
    assert_eq!(second_window.matches("RCPT TO").count(), 3);
}

/// The surplus check is deferred to the end of a window, not applied per
/// reply. A peer whose replies arrive coalesced is normal; only bytes left
/// after the whole group has drained prove it spoke out of turn.
#[test]
fn coalesced_window_replies_are_checked_once_at_the_group_boundary() {
    let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
    let mut harness = Harness::positive(8);
    harness.run(&mut machine).expect("the send succeeds");

    let group: Vec<&str> = harness
        .ops
        .iter()
        .map(String::as_str)
        .take_while(|op| *op != "CLOSE")
        .collect();
    assert_eq!(
        group,
        vec![
            "W:MAIL FROM:<sender@example.com>|RCPT TO:<rcpt-0@example.com>|RCPT TO:<rcpt-1@example.com>|",
            "OPEN",
            "READG",
            "READG",
            "READG"
        ],
        "every reply in a window is a grouped read, with no surplus check \
         between them: a coalesced pair is normal, not an unsolicited reply"
    );
}

/// A rejected `MAIL FROM` opened no transaction: the outstanding window
/// replies still have to drain, but no RSET is sent and the connection is
/// neither reset nor aborted.
#[test]
fn a_rejected_pipelined_mail_from_drains_the_window_without_reset_or_abort() {
    let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let error = harness
        .run(&mut machine)
        .expect_err("MAIL FROM was rejected");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert_eq!(
        harness.ops.iter().filter(|op| *op == "READG").count(),
        3,
        "both recipient replies are drained: {:?}",
        harness.ops
    );
    assert!(
        !harness.ops.iter().any(|op| op.starts_with("W:RSET")),
        "a rejected MAIL FROM opened no transaction: {:?}",
        harness.ops
    );
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "the connection stays reusable: {:?}",
        harness.ops
    );
    assert_eq!(harness.ops.last().map(String::as_str), Some("CLOSE"));
}

/// Finding 2: a routine negative reply after an accepted `MAIL FROM` is
/// cleared with RSET and the connection survives. Only a peer that fails to
/// acknowledge the reset costs the connection.
#[test]
fn a_rejected_recipient_resets_and_keeps_the_connection() {
    for pipelined in [false, true] {
        let mut machine = DirectSmtp::new(mail(), rcpts(1), pipelined, BodyKind::Data);
        let mut harness = Harness::new(vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(rejection()),
            OpOutcome::Reply(ok_reply()),
        ]);
        let error = harness
            .run(&mut machine)
            .expect_err("the recipient was rejected");

        assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
        assert!(
            harness.ops.iter().any(|op| op == "W:RSET|"),
            "pipelined={pipelined}: {:?}",
            harness.ops
        );
        assert!(
            !harness.ops.iter().any(|op| op == "ABORT"),
            "an acknowledged RSET keeps the connection: {:?}",
            harness.ops
        );
    }
}

#[test]
fn an_unacknowledged_reset_aborts_the_connection() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        // The RSET itself is refused.
        OpOutcome::Reply(rejection()),
    ]);
    harness
        .run(&mut machine)
        .expect_err("the recipient was rejected");

    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("ABORT"),
        "a refused RSET leaves the connection unusable: {:?}",
        harness.ops
    );
}

/// An I/O failure is not a protocol answer: every boundary that hits one
/// aborts the connection rather than resetting it, and carries the phase of
/// the boundary it failed at.
#[test]
fn an_io_failure_aborts_instead_of_resetting() {
    for (script, phase) in [
        (vec![network_failure()], SmtpCommandPhase::MailFrom),
        (
            vec![OpOutcome::Reply(ok_reply()), network_failure()],
            SmtpCommandPhase::RcptTo,
        ),
        (
            vec![
                OpOutcome::Reply(ok_reply()),
                OpOutcome::Reply(ok_reply()),
                network_failure(),
            ],
            SmtpCommandPhase::DataCommand,
        ),
    ] {
        let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
        let mut harness = Harness::new(script);
        let error = harness.run(&mut machine).expect_err("the transport failed");

        assert_eq!(error.phase(), Some(phase));
        assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
        assert!(
            !harness.ops.iter().any(|op| op.starts_with("W:RSET")),
            "a broken stream cannot be reset: {:?}",
            harness.ops
        );
    }
}

/// BDAT skips the `DATA` command entirely, frames the chunk itself, and
/// carries its own phase on a body failure.
#[test]
fn bdat_sends_no_data_command_and_carries_its_own_phase() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Bdat);
    let mut harness = Harness::positive(4);
    harness.run(&mut machine).expect("the send succeeds");
    assert!(
        !harness.ops.iter().any(|op| op == "W:DATA|"),
        "BDAT replaces DATA: {:?}",
        harness.ops
    );
    assert!(harness.ops.iter().any(|op| op == "BDAT"));

    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Bdat);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        network_failure(),
    ]);
    let error = harness
        .run(&mut machine)
        .expect_err("the chunk upload failed");
    assert_eq!(error.phase(), Some(SmtpCommandPhase::BdatBody));
}

/// LMTP answers once per ACCEPTED recipient after the body, in envelope
/// order, and the drain retires the connection either way.
#[test]
fn lmtp_reads_one_final_status_per_accepted_recipient() {
    let mut machine = DirectLmtp::new(mail(), rcpts(3), BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(reply(Severity::PositiveCompletion, "delivered-0")),
        OpOutcome::Reply(reply(Severity::PositiveCompletion, "delivered-2")),
    ]);
    let statuses = harness.run(&mut machine).expect("the delivery completes");

    assert_eq!(statuses.len(), 3);
    assert_eq!(statuses[0].message().next().unwrap(), "delivered-0");
    assert!(
        !statuses[1].is_positive(),
        "the RCPT rejection is preserved"
    );
    assert_eq!(statuses[2].message().next().unwrap(), "delivered-2");

    let grouped = harness.ops.iter().filter(|op| *op == "READG").count();
    assert_eq!(
        grouped, 2,
        "one status per accepted recipient: {:?}",
        harness.ops
    );
    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("DRAIN:true"),
        "the drain closes the group: {:?}",
        harness.ops
    );
}

/// The LMTP batch path records a per-recipient final status against the
/// ORIGINAL recipient index, skipping the rejected ones, and never restores
/// the stream: an LMTP connection is retired rather than pooled.
#[test]
fn lmtp_batch_finals_land_on_the_accepted_recipient_indexes() {
    let progress = SendProgress::new(Protocol::Lmtp, batch_recipients(3));
    let mut machine = BatchLmtp::new(mail(), rcpts(3), progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let progress = harness.run(&mut machine).expect("the batch completes");
    let outcome = progress.resolve();

    assert_eq!(outcome.succeeded().len(), 1, "one recipient was delivered");
    assert_eq!(
        outcome.failed().len(),
        2,
        "the RCPT rejection and the failed final status"
    );
    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("DRAIN:false"),
        "an LMTP batch never restores the stream: {:?}",
        harness.ops
    );
}

/// A DATA-command rejection after RCPT acceptances is per-recipient, never a
/// batch-level error: a batch-level error would let the engine resend a
/// non-idempotent send the server already refused.
#[test]
fn a_rejected_data_command_fails_recipients_rather_than_the_batch() {
    for protocol in [Protocol::Smtp, Protocol::Lmtp] {
        let script = vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(rejection()),
            OpOutcome::Reply(ok_reply()),
        ];
        let progress = SendProgress::new(protocol, batch_recipients(1));
        let mut harness = Harness::new(script);
        let progress = if protocol == Protocol::Smtp {
            let mut machine = BatchSmtp::new(mail(), rcpts(1), false, progress);
            harness.run(&mut machine)
        } else {
            let mut machine = BatchLmtp::new(mail(), rcpts(1), progress);
            harness.run(&mut machine)
        }
        .expect("a rejected DATA is not a batch-level failure");

        assert_eq!(progress.resolve().failed().len(), 1);
        assert!(
            harness.ops.iter().any(|op| op == "W:RSET|"),
            "{protocol:?}: the transaction is cleared: {:?}",
            harness.ops
        );
        assert!(!harness.ops.iter().any(|op| op == "ABORT"));
    }
}

/// Before DATA nothing can have been delivered, so a transport drop leaves
/// every unanswered recipient `Unsent` rather than uncertain - including
/// recipients the server already accepted.
#[test]
fn a_drop_before_data_marks_recipients_unsent() {
    let progress = SendProgress::new(Protocol::Smtp, batch_recipients(2));
    let mut machine = BatchSmtp::new(mail(), rcpts(2), false, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        network_failure(),
    ]);
    let progress = harness.run(&mut machine).expect("recipient outcomes stand");
    let outcome = progress.resolve();

    assert!(outcome.succeeded().is_empty());
    assert_eq!(outcome.failed().len(), 2, "no lane may claim a delivery");
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}

/// A rejected batch `MAIL FROM` is a batch-level error and costs the
/// connection: unlike the direct path, the batch driver aborts.
#[test]
fn a_rejected_batch_mail_from_aborts() {
    let progress = SendProgress::new(Protocol::Smtp, batch_recipients(1));
    let mut machine = BatchSmtp::new(mail(), rcpts(1), false, progress);
    let mut harness = Harness::new(vec![OpOutcome::Reply(rejection())]);
    let (error, _progress) = harness
        .run(&mut machine)
        .expect_err("a batch-level failure");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert!(matches!(error.kind(), ErrorKind::Permanent(_)));
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}
