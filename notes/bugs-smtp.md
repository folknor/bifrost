# bifrost-smtp: hunt findings

Scope: `crates/smtp/` - transport types (async and blocking halves, both published
surface), connection pooling, PIPELINING, DSN, message builder, LMTP, TLS handling,
examples.

Hunter note: baseline `brokkr check -p smtp` passes clean (exit 0). Every finding
below is a defect in code that currently compiles and passes its own suite.

## 1. STARTTLS plaintext-injection: buffered pre-TLS bytes survive the upgrade

**Confidence: high. Both halves. Security.**

`AsyncSmtpConnection::starttls` (`client/async_connection.rs:1529`) and
`SmtpConnection::starttls` (`client/connection.rs:1361`) do:

```rust
try_smtp!(self.command(Starttls).await, self);
self.stream.get_mut().upgrade_tls(tls_parameters, self.timeout).await?;
try_smtp!(self.hello(hello_name).await, self);
```

`self.stream` is a `BufReader<AsyncNetworkStream>`. `get_mut()` swaps the *inner*
stream for the TLS stream and leaves the BufReader's buffer completely untouched.
Nothing checks or discards it.

The reply read for `220 Ready to start TLS` goes through `read_until` on the
BufReader, which fills the buffer with whatever the socket had available -
potentially more than that one line. A MITM (or a compromised/hostile relay) appends
plaintext after the `220`; those bytes sit in the buffer across the upgrade and are
then consumed as the *first bytes of the TLS session*. That includes the
post-STARTTLS `EHLO` reply, which is what populates `ServerInfo` - so the attacker
chooses the advertised extension set, the AUTH mechanism list, the SIZE limit, and
DSN support, from outside TLS. This is the classic STARTTLS response-injection class
(CVE-2011-0411 and the 2021 "NO STARTTLS" family), present here in textbook form.

The fix is one line and the crate already has the primitive:
`finish_lmtp_final_drain` uses `self.stream.buffer().is_empty()`. Assert the same
thing immediately before `upgrade_tls` and hard-fail (mark `Broken`, abort) if it is
non-empty. Both halves.

Two aggravating details:

- `try_smtp!(self.command(Starttls)...)` uses `command`, which reads via
  `read_response_with_budget` -> `accept_negative: false`, so a negative reply errors
  out. Good. But the buffer is still not checked on the success path.
- The test `starttls_downgrade_is_refused_without_a_wire_command` and its siblings
  stop at the handshake boundary by design (`reference/smtp.md` says so), which is
  exactly why this was never observed - the transcript harness never gets to model a
  post-220 injection. The harness *can* model it:
  `expect_coalesced("STARTTLS\r\n", "220 go ahead\r\n250-attacker...\r\n")` plus a
  buffer assertion is a hermetic regression test.

## 2. Reply desynchronization is pinned as behavior instead of fixed - mail can be mis-attributed

**Confidence: high. Both halves.**

`async_connection.rs:3566`,
`unsolicited_reply_coalesced_with_an_answer_desynchronizes_the_next_command`, whose
own doc comment says: *"This pins the observable consequence rather than pretending
the desync cannot happen."* The test asserts that a `421` coalesced behind a `250
noop ok` is consumed as the *next* command's reply.

That is not a harmless curiosity on a pooled connection. Once the stream is one reply
ahead, every subsequent reply is mis-attributed by one command, and the connection is
*recycled into the pool* because nothing marks it. Concretely, on the next send
through that connection:

- `MAIL FROM` reads the stale reply. If the stale reply was positive, `MAIL FROM`'s
  real (possibly negative) reply is later read as the `RCPT TO` answer, and so on down
  the chain.
- The shift walks all the way to `DATA`-final. A stale `250` read as the DATA-final
  acceptance means **`SendProgress` records a successful delivery for a message the
  server never accepted** - silent mail loss reported as success. The mirrored case (a
  stale negative shifted onto the final) produces a spurious failure for a message
  that *was* accepted, which the engine will retry: **duplicate mail** on a
  non-idempotent `Send`.

LMTP already has the correct defense and the correct reasoning written down
(`finish_lmtp_final_drain`, plus unconditional retirement). SMTP has neither. The fix
is the same primitive: after every reply read completes, if `self.stream.buffer()` is
non-empty, the peer has spoken out of turn - set `ConnectionState::Broken` and refuse
to recycle. This costs one `is_empty()` per reply and closes the whole class. The
pipelining drains would need the check deferred to the end of each window (where extra
buffered replies are expected and counted), which the window loop already has a
natural place for.

The hunter classifies this as the single most consequential finding after the STARTTLS
one, because it is the only one that can lose or duplicate mail *silently*.

## 3. Batch send never checks recipients for non-ASCII - SMTPUTF8 is skipped for EAI recipients

**Confidence: high. Both halves. Divergence between the two code paths.**

`mail_options` (single-envelope) at `async_connection.rs:1280`:

```rust
if envelope.has_non_ascii_addresses() && !has_smtputf8 {
```

`has_non_ascii_addresses()` covers the reverse-path *and* every forward-path
recipient.

`mail_options_for_batch` at `async_connection.rs:1200` (and `connection.rs:1040`):

```rust
let has_non_ascii = from.is_some_and(|a| !AsRef::<str>::as_ref(a).is_ascii());
if has_non_ascii && !has_smtputf8 {
```

Only the sender. The recipients are never examined, even though they are right there
in `progress.recipients`.

Consequences on the account-oriented batch path (which is the path `bifrost-sync`
actually drives):

- Against a server **without** SMTPUTF8: the guard that is supposed to refuse the send
  never fires, and `RCPT TO:<üser@example.com>` goes on the wire raw. RFC 6531 section
  3.4 forbids this. Real-world outcome is a `501` per recipient at best, silent
  local-part mangling at worst.
- Against a server **with** SMTPUTF8: the `SMTPUTF8` MAIL parameter is not emitted, so
  a conforming server is entitled to reject the transaction it would otherwise have
  accepted.

Either way the single-envelope `send_raw*` path and the batch path give different
answers for the same envelope, which is the strongest signal that this is an oversight
rather than a decision. Fix: pass the recipient addresses into
`mail_options_for_batch` and OR them into `has_non_ascii`.

The reason this bug exists is the structural finding below: `mail_options` and
`mail_options_for_batch` are near-verbatim 80-line copies, times two for sync/async -
four copies, and the fix landed in two of them.

## 4. `abort()` has no timeout and can hang the caller past every configured deadline

**Confidence: medium-high. Both halves, worse on async.**

```rust
pub(crate) async fn abort(&mut self) {
    let _ = self.stream.shutdown().await;
}
```

For a `TokioNativeTls` stream, `poll_shutdown` sends `close_notify` and waits for the
peer's. There is no `with_timeout` around it - unlike literally every other I/O in the
connection driver, which all route through `with_timeout(budget, ...)`.

`abort()` is awaited on the error arm of essentially every send path:
`send_smtp_batch` (four sites), `send_smtp_batch_pipelined` (six sites),
`send_lmtp_batch` (five sites), `auth_scram`, `auth_legacy`, `test_connected`, and the
pool's checkout and recycle. A peer that accepts the TCP connection and then stops
responding - precisely the case `Transcript::silent()` and `expect_then_stall` were
built to model - makes `abort()` never return. The caller's per-operation timeout has
already fired and been converted into an error; the code then awaits `abort()` on the
way out and hangs *after* the timeout it was supposed to honor. From the caller's side
this looks like the timeout not working at all.

The same hang parks the pool: `Pool::recycle` and the cleanup worker both await
`abort()`.

Fix: wrap the shutdown in the per-operation budget and fall through to a hard drop on
expiry. A close that the peer will not acknowledge is not worth waiting on.

## 5. `abort_concurrent` is sequential - one hung peer blocks the whole pool

**Confidence: high (trivially verifiable). Compounds 4.**

`pool/async_impl.rs:370`:

```rust
async fn abort_concurrent<I>(iter: I) where I: Iterator<Item = AsyncSmtpConnection> {
    for mut conn in iter { conn.abort().await; }
}
```

A `for` loop awaiting each element in turn is the definition of *not* concurrent. The
name is a lie, and combined with 4 it means one unresponsive peer stalls
`Pool::shutdown()`, the `Drop` cleanup task, and every expiry sweep in the cleanup
worker - head-of-line blocking across every other connection in the pool.

Fix is `futures::future::join_all` / `FuturesUnordered`, or keep the loop and rename it
honestly. Given 4 exists, fix the concurrency rather than the name.

## 6. `PoolConfig::max_size` does not bound connections - only idle parking

**Confidence: high.**

`Pool::connection()` (`pool/async_impl.rs:159`) on an empty idle list goes straight to
`self.client.connection().await?` with no admission control whatsoever. `max_size` is
consulted in exactly one place - `recycle`, at line 251 - where it decides whether a
*returning* connection is parked or aborted.

So `max_size` is a cap on the idle set, not on live connections. A caller sending
10 000 concurrent messages through a transport configured `max_size(4)` opens 10 000
sockets, and 9 996 of them are aborted on the way back. The field reads as a
connection limit - it is named `max_size` on a type called `PoolConfig` - and callers
will configure it as one, against a relay that enforces per-client connection limits,
and get their traffic refused or tarpitted with no indication why.

This is the `MutationConfig::retry_queue_cap` shape from the standing lessons: a field
that reads as a bound over something unbounded. Per that lesson, the fix that *keeps*
the item is the right one - make `max_size` an actual semaphore-backed checkout bound,
with `PooledConnection` releasing the permit on drop. Do not delete the field.

Related, lower severity: the cleanup worker's replenish loop (`for _ in
count..(min_idle as usize)`) counts only *parked* connections, so a busy pool dials
`min_idle` fresh connections on top of everything currently checked out.

## 7. `PooledConnection::drop` and `Pool::drop` spawn tasks - fire-and-forget teardown

**Confidence: medium.**

Both `Drop` impls call `E::spawn(...)` to do their work. Two consequences:

- `tokio::spawn` **panics** if no runtime is active. Dropping an `AsyncSmtpTransport`
  (or the last `PooledConnection`) outside a runtime context - during a shutdown
  sequence, in a `Drop` chain that runs after `Runtime::shutdown`, in a synchronous
  test teardown - panics in a destructor. Panicking in `Drop` during unwind aborts the
  process.
- Recycling is asynchronous and unordered relative to the caller. A caller that sends,
  drops the guard, and immediately sends again may not see the connection back yet,
  dials a second one, and the first arrives afterward. Correctness-neutral but it
  defeats pooling under exactly the sequential-send pattern pooling exists for, and it
  makes the pool's observable state untestable without sleeping.

The structural fix is to stop doing I/O in `Drop`. Return the connection to a
synchronous parking list under a `std::sync::Mutex` (the recycle decision -
`has_broken() || should_retire()` - is pure and needs no await), and let the *next*
checkout or the cleanup worker perform the aborts.

## 8. Pipelined single-envelope send is systematically missing `SmtpCommandPhase` decoration

**Confidence: high. Both halves.**

Compare `send_with_options`'s non-pipelined branch, where every step is tagged:

```rust
try_smtp!(self.command(mail).await, self, SmtpCommandPhase::MailFrom);
try_smtp!(self.command(recipient).await, self, SmtpCommandPhase::RcptTo);
try_smtp!(self.command(Data).await, self, SmtpCommandPhase::DataCommand);
try_smtp!(self.message(email).await, self, SmtpCommandPhase::DataBody);
```

against `send_pipelined` (`async_connection.rs:417`), where:

- `self.write(commands.as_bytes()).await?` - bare `?`, no phase, no attempt state, no
  abort.
- every `read_response_with_budget_inner(...).await?` - bare `?`, no phase.
- `self.command_accepting_status(Data).await?` - bare `?`.
- `try_smtp!(self.message(email).await, self)` - two-arg form, **no phase**, where the
  non-pipelined twin passes `DataBody`.

`reference/smtp.md` states that phase lives on `Error::Inner` *"so a missed
`with_phase` decoration cannot silently degrade the classifier."* That guarantees the
phase cannot be *lost*; it does not supply one that was never attached. Since
`account_error.rs` reads `error.phase()` in preference to the context phase, and
PIPELINING is advertised by essentially every modern relay, **the phase-aware routing
is dark on the default production path for single-envelope sends.** A DATA-body
transport drop and a MAIL FROM write failure arrive at the classifier
indistinguishable.

The batch pipelined path (`send_smtp_batch_pipelined`) *is* decorated throughout,
which confirms the decoration is meant to be there.

## 9. Sync `send_pipelined` does not hold `Broken` across the window drain

**Confidence: high. Asymmetry the reference explicitly claims does not exist.**

The async version brackets each window:

```rust
self.stream.get_ref().state().verify()?;
self.stream.get_mut().set_state(ConnectionState::Broken);
... drain replies ...
self.stream.get_mut().set_state(ConnectionState::Ok);
```

The sync version (`connection.rs:259`) has none of it - it writes the window and
drains with no state bracketing at all.

`reference/smtp.md` says *"The two halves are held in step deliberately"* and *"the
fifteen invariants that only the blocking tests had pinned are now covered on BOTH
sides."* This invariant went the other way and is covered on neither. There is no
future to cancel in the sync half, so the async motivation does not transfer directly
- but a panic unwinding through the drain (a caller using `catch_unwind`, an
allocation failure, a `debug_assert` in a callee) leaves the connection `Ok` with N
undrained replies and hands it straight back to the pool. That is finding 2's failure
mode reached by a different door.

## 10. Dead branches in `send_smtp_batch_pipelined`, and a healthy connection thrown away

**Confidence: high on the dead code, medium on the severity of the abort.**

At `async_connection.rs:887-936`. The function computes `accepted`, and at line 893
returns early if `!accepted`. Therefore `accepted == true` for the remainder. Yet:

- line 919: `if !accepted { ... RSET ... } else { self.abort().await; }` - the `if` arm
  is unreachable.
- line 929: `if !accepted { self.write(b".\r\n") ... }` - an entire unreachable block
  that writes a bare DATA terminator.

Unreachable code that writes to the wire in a send driver is not a lint nit; it is a
leftover from a control-flow change, and the reachable half of it is wrong. On a
negative `DATA` reply the reachable branch is `self.abort().await` - but a negative
DATA reply leaves the connection **perfectly reusable**; the transaction is closed by
the rejection and an `RSET` restores it. The non-pipelined twin at line 739 does
exactly that:

```rust
progress.mark_accepted_rejected_with_response(resp);
if let Err(_e) = self.command_accepting_status(Rset).await { self.abort().await; }
```

So the pipelined batch path discards a healthy pooled connection on every DATA
rejection while the non-pipelined path keeps it. Since PIPELINING is near-universal,
the pipelined path is the one that runs.

## 11. `.expect()` on a server-controlled invariant in the LMTP direct-send paths

**Confidence: medium. Panic reachability is argued-away, not proven.**

`send_lmtp_with_options` (`async_connection.rs:554`) and
`send_lmtp_bdat_with_options` (line 622):

```rust
delivery_statuses.next().expect("server returned one status per accepted recipient")
```

This is safe *only* because `message_lmtp_iter` reads exactly `accepted_recipients`
responses and errors otherwise. That is currently true. But the invariant being
asserted is phrased as a fact about the server, in a library, on the delivery path, in
code reachable from a public entry point - and it is held together by a count computed
100 lines earlier in a different function. `error::internal(...)` is already used four
lines above for the sibling invariant (`"recipient status invariant failed after all
recipients were rejected"`), so the non-panicking idiom is right there in the same
function.

Note also that the `resolve()` path in `batch.rs` handles precisely this shape
correctly - `debug_assert!` plus a release-mode `Uncertain` fallback. The direct-send
path chose `expect`. Inconsistent treatment of the same hazard.

## 12. LMTP `accepted_recipients == 0` early return leaves an open transaction

**Confidence: medium.**

`send_lmtp_with_options` at line 526 and `send_lmtp_bdat_with_options` at line 599:
when every recipient was rejected, the function returns `Ok(rejected)` immediately -
`MAIL FROM` was accepted, no `DATA` was issued, and **no `RSET` is sent**. The
connection is returned to the pool with a half-open transaction. The next send through
it issues `MAIL FROM` into an already-open transaction and gets a `503 Bad sequence of
commands`.

`send_smtp_batch` has the identical hole at line 731 (`if !accepted { return
Ok(progress); }` - no RSET). The pipelined batch path at line 893 *does* RSET in this
case, and `send_lmtp_batch` at line 1047 does not. So of the four places that handle
"all recipients rejected", one gets it right.

For LMTP this is masked by the unconditional retirement rule - but only for
connections that reached a final-status drain, which this path by definition did not
(`retire` is still `false`, so it *does* go back to the pool). Confidence is medium
only because some servers implicitly reset on a fresh `MAIL FROM`; RFC 5321 section
4.1.1.5 does not require them to.

## 13. `AsyncSmtpClient::connection` drops a live connection without aborting on auth-policy refusal

**Confidence: low-medium.**

```rust
if let Some(credentials) = &self.info.credentials {
    self.info.ensure_can_authenticate(conn.is_encrypted())?;   // <- `?` drops conn
    conn.auth(&self.info.authentication, credentials).await?;
}
```

The `?` on `ensure_can_authenticate` drops `conn` without `abort()`. The socket closes
on drop, so no FDs leak - but the connection is torn down without `close_notify` and,
more to the point, this is the plaintext-AUTH refusal path, i.e. the one that fires
when a caller has misconfigured TLS. Cosmetic against a real server; noted because the
crate is otherwise scrupulous about aborting.

## Things checked that are correct

Recorded so the next pass does not re-derive them:

- **Dot-stuffing** (`ClientCodec::encode`, `client/mod.rs:97`) is correct including the
  awkward cases: bare LF treated as a line break (deliberate, tested), lone CR *not*
  treated as one (the `(_, StartingNewLine)` arm precedes `(b'.', StartOfNewLine)`, so
  `\r.` is not stuffed - right), state carried across chunk boundaries, and a dot at
  byte 0 of the message stuffed.
- **`smtp_data_size`** correctly excludes transparency dots and the terminator line per
  RFC 1870 section 3, and correctly reuses an existing final CRLF. The `line\r` -> 7 and
  `line\n` -> 7 cases are right.
- **Header injection via header names** is closed by `is_ftext` (`header/mod.rs:189`),
  and the comment explaining why is accurate.
- **Header injection via header values** is closed, though not where the comment says.
  `allowed_char` (line 394) excludes 0, 10, and 13, so any word containing CR or LF is
  diverted into `encode_buf` and RFC 2047 encoded. `Subject`, `Comments`, `Keywords`
  etc. accept unvalidated `String` through `text_header!`, and are safe only because of
  this encoder property. That is load-bearing behavior with no test naming it as such -
  a test asserting `Subject::from("x\r\nBcc: attacker@e.com")` round-trips to an
  encoded-word would be cheap insurance.
- **VRFY / EXPN / MAIL FROM / RCPT TO** all route through
  `validate_single_line_argument` with `char::is_control`, covering the unchecked
  `Address` constructor as the reference claims.
- **`MailParameter::OtherRaw`** does validate its keyword and value
  (`validate_esmtp_keyword` / `validate_esmtp_raw_value`) despite the "Raw" name.
- **`Tls::Required`** genuinely cannot be stripped: it calls `conn.starttls(...)`
  unconditionally, which errors if `Extension::StartTls` is not advertised.
  `Tls::Opportunistic` is strippable by definition and is documented as such.
- **`ServerInfo` is replaced** by the post-STARTTLS and post-AUTH EHLO, so pre-TLS
  advertisements do not leak forward - *except* through the buffer hole in finding 1.
- **LMTP per-recipient accounting** in `batch.rs` `SendProgress::resolve` is sound:
  lane order follows input order, RCPT rejections are preserved at their original
  index, `Pending` at resolve becomes `Uncertain` rather than vanishing, and the
  `Accepted`-without-`Final` case has both a `debug_assert!` and a release fallback.
- **The `Unsent` vs `Uncertain` split** is correct everywhere traced. Envelope-phase
  failures resolve through `mark_unresolved_unsent`, including the later-pipelining
  window write failure, and `Accepted` is rewritten alongside `Pending` for the reason
  the comment gives. `InFlight` starts at `DATA`. This is the part of the crate in the
  best shape.

## Structural story

### The central problem: `connection.rs` and `async_connection.rs` are a 6,600-line copy of each other

3,053 lines and 3,592 lines, same module layout, same function names, same comments,
same tests, differing only in `.await` and `Read`/`Write` versus
`AsyncRead`/`AsyncWrite`. Together they are 22% of the crate.

Five of the thirteen findings above are *directly caused by this*, in two shapes:

- **The fix landed on one side only.** Finding 9 (sync pipelining has no `Broken`
  bracket) is an async-side fix that never crossed. The reference document asserts the
  two halves are in step; they are not, and nothing mechanically checks the claim.
- **The fix landed on both sides but only in one of the two copies each side has.**
  Finding 3 (SMTPUTF8 recipients) exists in `mail_options_for_batch` on both halves,
  while `mail_options` - sitting 50 lines below it in the same file - is correct.
  Finding 12 (missing RSET) is correct in one of four analogous sites.

That second shape is the more damning one, because it says the duplication is not just
sync-versus-async. Within each half there are already two copies of the option-building
logic (single-envelope and batch) and two copies of the send driver (plain and
pipelined), so the same rule is written four times per half and eight times across the
crate. Every one of those eight has to be edited in lockstep, forever, by hand, with
the reference doc as the only checker.

Explicitly, given the history: **the hunter is not proposing removing the blocking
half.** It is published API, it was deleted once on bad reasoning, and it must stay.
The proposal is the opposite - it is precisely *because* both halves must be maintained
indefinitely that maintaining them as two independent transcriptions is the wrong
shape.

The shape it would build instead:

1. **Extract the SMTP protocol driver as a sans-I/O state machine.** One `SmtpDriver`
   that owns `ServerInfo`, `ConnectionState`, the `retire` flag, `SendProgress`, the
   pipelining window bookkeeping, the LMTP final-status accounting, all option
   construction and validation, and all phase/attempt decoration. Its interface is `fn
   step(&mut self, event: Event) -> Action`, where `Event` is `{ ReplyRead(Response),
   WriteCompleted, Eof, Timeout }` and `Action` is `{ Write(Bytes), ReadReply, Abort,
   Done(Outcome) }`. No `async`, no `Read`, no sockets. Every decision in findings 3, 8,
   10, 12 lives here, once.
2. **Two thin I/O pumps** - roughly 200 lines each, sync and async - that loop over
   `step`, perform the requested `Write`/`ReadReply` against `NetworkStream` /
   `AsyncNetworkStream` with the appropriate timeout budget, and feed the result back.
   Cancellation safety becomes a property of the pump (~200 lines to audit), not of
   3,600 lines of interleaved protocol-and-I/O.
3. **The buffer-cleanliness invariant becomes a driver postcondition**, not something
   each call site remembers. `ReadReply` returns "reply plus whether the buffer is now
   empty," and the driver decides. Findings 1 and 2 both close structurally rather than
   by remembering to add an `is_empty()` at N sites.
4. **The `Transcript` harness tests the driver directly** - no I/O, no runtime, no
   `start_paused` time. It becomes a pure `Vec<Event> -> Vec<Action>` comparison, which
   is faster, more deterministic, and - critically - **tests both halves at once**,
   because there is only one thing to test. The current 15-invariants-mirrored-on
   both-sides test duplication, which the reference calls "the intended end state, not
   debt to pay down," stops being necessary: the duplication exists to compensate for
   the production duplication, and it disappears with its cause. The two pumps still
   need their own small cancellation and timeout suites; that is the right amount of
   duplicated testing.

This is a large rewrite and the hunter recommends it directly. The payoff is not
tidiness - it is that the class of bug that produced five of the thirteen findings
stops being expressible.

### Secondary: the pool is a parking lot, not a pool

Findings 5, 6, and 7 are one design issue seen three ways. `Pool` has no admission
control, does I/O in `Drop`, and calls a sequential loop "concurrent." What it actually
implements is an idle-connection parking lot with an expiry sweeper.

Reshape it as a real pool: a semaphore sized by `max_size` acquired at checkout and
released by `PooledConnection`'s `Drop`; a synchronous parking list so `Drop` never
spawns; and aborts batched and performed by whoever next touches the pool rather than
by a detached task per connection. `min_idle > 0` keeps the warm-connection worker,
which is fine as-is once the counting bug is fixed. This also makes the pool's state
synchronously observable, so `idle_count_for_test` stops being a race.

### Third: `SmtpErrorContext` reconstruction at every call site

The batch drivers contain this, verbatim, at fifteen separate sites:

```rust
use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
let ae = into_account_error(
    e.with_attempt(SmtpTransmissionState::InFlight).with_phase(SmtpCommandPhase::DataBody),
    SmtpErrorContext::send(Protocol::Smtp).with_phase(SmtpCommandPhase::DataBody),
);
let ae2 = ae.clone();
progress.mark_uncertain_unresolved(|| ae2.clone());
```

including a function-body `use` statement repeated fifteen times, the phase written out
twice per site, and a clone-into-a-closure dance to work around the `impl Fn()`
signature. Every one of these is a place where the phase can be written inconsistently
between the error and the context, and finding 8 is what happens when someone skips the
block entirely. This wants to be `progress.fail(phase, attempt, e)` on `SendProgress`,
taking the phase once. In the sans-I/O shape it disappears into the driver.

## Lateral observations

- **`Ehlo::new` / `Lhlo::new` do not validate their `ClientId`**; `hello_with_budget`
  calls `hello_name.validate()?` first, so the live path is safe. Any future call site
  that constructs `Ehlo` directly and hands it to `command()` would not be. Cheap to
  move the validation into the constructor, where the other commands put it.
- **`command_buffer` zeroization has a documented gap** (`reference/smtp.md`: a
  `write!` that outgrows the allocation leaves one stale heap copy). Accurate.
  `String::reserve` to a fixed high-water mark once at connection setup would close it,
  at the cost of a few hundred bytes per connection.
- **`test_on_checkout` is documented as buying nothing for LMTP** - correct, since LMTP
  connections are retired at recycle and never checked out idle. But `PoolConfig` is
  shared between the SMTP and LMTP transports with no way to express that, so an LMTP
  caller can set a field that is silently inert. A `PoolConfig` that knows its protocol,
  or a debug-assert, would be honest.
- **`Pool::new`'s cleanup worker sleeps `idle_timeout` between sweeps**, so a
  connection can sit up to `2 x idle_timeout` past expiry before the worker drops it.
  Checkout catches it (`connection()` checks `idle_duration` first), so this only
  affects when FDs are released, not correctness.
- **`error_from_status` is a one-line passthrough** to `error::status` and adds
  nothing. Trivial, but it is the kind of vestigial indirection that makes the two
  halves look more different than they are when diffed.

## Out of scope, flagged

Nothing in `crates/types/` or `crates/net/` surfaced as suspect from the SMTP side.

`reference/smtp.md` line 30 states the sync/async test duplication is "the intended end
state, not debt to pay down." Finding 9 is a counterexample - the duplication did not
catch a missing invariant on the sync side, because duplicated *tests* only help when
the invariant was written down twice to begin with. If the sans-I/O reshape is not
taken, the reference's claim should at least be downgraded, and something mechanical
(even a script comparing function-name sets across the two files) should back it.

`reference/smtp.md` lines 18-32 and 46-52 assert several invariants ("the two halves are
held in step", phase decoration cannot degrade the classifier) that findings 3, 6 and 9
contradict in the current code. That doc is in the durable, must-be-true tier, so it
needs correcting alongside whichever fixes land.
