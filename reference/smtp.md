# bifrost-smtp reference

Current architecture of the SMTP client crate. Descended from lettre 0.11.22; the bifrost fork has substantially diverged. Native-tls only.

## Transport types

Sync and async, SMTP and LMTP:

- `SmtpTransport` / `AsyncSmtpTransport` - submission to MX/relay.
- `LmtpTransport` / `AsyncLmtpTransport` - local delivery via LMTP.

`Transport::Ok` is `Response` for SMTP, `Vec<Response>` for LMTP (one per envelope recipient, original order preserved).

Async transports support Tokio. TLS and plaintext URL parsing via `from_url` use the Tokio backend.

`BoxedTransport<Ok, Error>` and `BoxedAsyncTransport<Ok, Error>` erase the concrete type. `Box<T>` and `Arc<T>` get blanket `Transport` / `AsyncTransport` forwarding.

**The blocking half stays. Do not re-delete it.** `SmtpTransport`,
`LmtpTransport`, the `Transport` trait, the blocking pool and socket funnel,
`oauth2_token_blocking`, the blocking examples and the `tokio` cargo feature were
once removed wholesale on the reasoning that the only IN-WORKSPACE consumer is
async, and were restored at the repository owner's instruction. That reasoning
does not hold for a library crate, whose consumers are outside this workspace by
definition; "nothing calls it" established by grepping here is a fact about the
workspace, not about who uses `SmtpTransport`.

The two halves no longer mirror a protocol state machine each: the send paths
share one sans-I/O core and differ only in how they move bytes. See "Protocol
core and I/O adapters". What still exists twice - connect, greeting, EHLO/LHLO,
the AUTH ladder, STARTTLS, reply reading, deadlines - is I/O-shaped, and the
paired tests over it remain intentional while the published sync and async
transports remain separate. The deliberate behavioural differences are the
`abort()` timeout (see "Connection lifecycle") and the outbound-throttle
invariant (see "Bandwidth metering"). Any proposal to remove or reshape this
surface is the owner's call - see the standing lessons in `AGENTS.md`.

## Protocol core and I/O adapters

`client/core.rs` is the sans-I/O protocol core for every send path. It owns
command sequencing, reply-group accounting (the PIPELINING window and the LMTP
final-status drain), `SmtpCommandPhase` decoration, `SendProgress` transitions,
and the RSET-and-keep versus abort decision. It takes typed outcomes in and
yields byte writes plus typed ops out. It never touches a socket, a timeout or
a clock, so it can be driven to completion with no I/O at all.

The shape is a step function, not a straight-line routine, because the two
adapters cannot share control flow - one blocks, one awaits. A machine is asked
for its next `Op`; the adapter performs it and feeds back an `OpOutcome`
(`Done`, `Reply(Response)`, or `Failed(Error)`). A negative reply is a
`Reply`, never a `Failed`: telling a routine rejection apart from a transport
failure is exactly the decision the core exists to make in one place.

Four machines cover the send surface, and each is what one driver method used
to be:

| Machine | Drives |
|---|---|
| `DirectSmtp` | `send_with_options` / `send_bdat_with_options`, pipelined or not |
| `DirectLmtp` | `send_lmtp_with_options` / `send_lmtp_bdat_with_options` |
| `BatchSmtp` | `send_smtp_batch`, pipelined or not |
| `BatchLmtp` | `send_lmtp_batch` |

The op vocabulary is small and every variant maps to a primitive both drivers
already had: `Write(String)`, `WriteBody`, `WriteBdat`, `OpenReplyGroup`,
`ReadGrouped`, `ReadSingle`, `CloseReplyGroup`, `CloseLmtpDrain { restore_ok }`,
`Abort`. `OpenReplyGroup` / `CloseReplyGroup` are the reply-group bracket: the
stream is held `Broken` from the group's first read until its last has drained,
so a connection abandoned mid-drain can never be recycled. `ReadGrouped` defers
the surplus check to the close; `ReadSingle` applies it itself. A command write
is followed by the reply read for it, and a write failure is handed to the same
stage that would have handled the reply, so both carry that boundary's phase.

The `Epilogue` type holds the two terminal sequences every machine shares. It
carries the value the machine will finish with, so a boundary cannot start an
abort or a reset and forget to return. RSET-and-keep is expressed there as ops -
write `RSET`, read the reply, keep the connection on a positive
acknowledgement and abort otherwise - so no adapter knows the rule.

Each adapter is a `drive` loop plus a `perform` match, roughly thirty lines.
What stays adapter-side is what is genuinely I/O: dialling, the greeting and
EHLO/LHLO exchange, the AUTH ladder, the STARTTLS upgrade, reply-line reading
and parsing with its size caps, read and write deadlines, bandwidth throttling,
and the socket shutdown `Op::Abort` performs. The local validation that runs
before `MAIL FROM` - `build_envelope` and `build_batch` - also stays with the
adapter, because it reads `ServerInfo` capabilities off the live connection.

## Connection lifecycle

```
greeting -> EHLO/LHLO -> [STARTTLS -> EHLO/LHLO] -> [AUTH -> EHLO/LHLO] -> ready
```

EHLO is re-sent after STARTTLS and AUTH because capabilities can change.

STARTTLS upgrades are fail-closed at the buffered-reader boundary. If the peer
coalesces any plaintext bytes after its positive STARTTLS reply, the connection
is marked broken and the upgrade is refused. Pre-TLS bytes can never become a
post-TLS reply.

- `abort()` closes the connection and marks it `Broken` on both sides, so an
  aborted connection can never pass `state().verify()` again. There is no live
  QUIT path; the Quit command builder is gone. If a graceful shutdown is wanted
  later it will need to be re-added with a real call site.
- The async `abort()` is bounded by the per-operation timeout. `poll_shutdown`
  on a TLS stream sends `close_notify` and waits for the peer's, so an
  unresponsive peer would otherwise hang the caller *after* the timeout that
  already fired. The blocking `abort()` needs no such bound: `Shutdown::Both`
  on a blocking socket is a syscall that returns immediately. This is the one
  place the two halves differ by design rather than by omission.

## Connection state and cancel-safety

Sync and async streams carry an explicit state: `Ok` / `Broken` / `Closed`. Writes, flushes, reads, and TLS upgrades set `Broken` before the await; only success restores `Ok`. Dropped futures leave state `Broken`; the pool drops the connection.

LMTP final delivery-status loop holds `Broken` until every accepted recipient's status has been read. A surplus final status that already reached the read buffer is a protocol violation: the connection is marked `Broken` and the drain fails (the batch driver keeps the per-recipient outcomes it already has, since the surplus changes only the stream's reusability).

The direct and batch LMTP paths differ in one respect, preserved rather than
unified when the drain moved into the core (`Op::CloseLmtpDrain { restore_ok }`):
a direct LMTP send restores the stream to `Ok` after a clean drain, while an
LMTP batch leaves it `Broken`. Neither connection is reusable in practice, since
every drain retires it, so this is a difference in bookkeeping rather than in
behaviour - but it is a real difference and it is now visible in one place.

Surplus bytes that have not yet crossed into the `BufReader` - still in the socket or a TLS record - cannot be observed without a read that would block against a well-behaved peer, and native-tls exposes no nonblocking peek that would make such a probe honest. So cleanliness is never positively established: every LMTP final-status drain sets a retirement flag (`should_retire()`), and the pool discards those connections at recycle instead of parking them. The cost is one reconnect per LMTP transaction; LMTP is local delivery, so that is cheaper than recycling a possibly desynchronized stream. SMTP connections are unaffected and still pool normally.

`test_connected()` (pooled NOOP probe) aborts on failure so a stale connection cannot be recycled.

An ordinary SMTP reply must also end with an empty read buffer. Surplus bytes
prove that the peer spoke out of turn, so the driver marks the connection broken
and reports a parse error before another command can consume the stale reply.
PIPELINING and LMTP final-status groups defer this check until their exact
expected reply count has drained.

Envelope commands are constructed, and therefore validated, before `MAIL FROM`
is written. Every send path - sync and async, DATA and BDAT, SMTP and LMTP,
pipelined and not, single-envelope and batch - builds the whole `Mail` plus
`Rcpt` set up front (`build_transaction_commands` / `build_recipient_commands`)
and only then opens the transaction. This is a connection-state invariant, not
a stylistic one: a construction failure raised from inside the transaction
would unwind through the caller's `?` without running the abort that a
wire-level failure runs, so the connection would stay `Ok`, return to the pool,
and hand the next send a half-open transaction. Building first keeps every
address rejection on the clean side of `MAIL FROM`, where no abort is needed
and the connection stays reusable.

## Async setup deadline

`AsyncDeadline` is a single shared deadline across DNS, connect, TLS handshake, banner read, and initial EHLO. The deadline lives only until `connect_impl` returns; established connections use the per-operation timeout. `starttls(...)` on an established connection uses the per-operation timeout because it is an explicit command, not setup.

It measures on `tokio::time::Instant`, not `std`'s. Every timeout the deadline
hands out is a `tokio::time::timeout`, and a deadline that reads a different
clock from the timers it arms is only accidentally right - it disagrees
wherever tokio's clock is not the system one, which under `start_paused` test
time meant the deadline never expired at all.

## Per-reply read deadline

The configured timeout bounds one REPLY, not each line of it. Both drivers
enforce that, by different mechanics for the same reason: the natural unit on
each side is smaller than a reply, so a multi-line reply armed naively gets a
fresh full timeout per line and a peer trickling one line per period stretches
one reply to `MAX_RESPONSE_BYTES / line` times the configured timeout, bounded
only by the size caps.

- The async reader collapses a `PerOperation` budget into an `AsyncDeadline` on
  entry and draws every line's `tokio::time::timeout` from the slack left on
  it. A `SetupDeadline` budget is already deadline-shaped and passes through,
  so the connect path keeps its one shared deadline.
- The blocking reader has only `SO_RCVTIMEO`, which is per read syscall and has
  no getter, so `SmtpConnection` keeps the configured value in `read_timeout`
  and re-arms the socket with the remaining slack before each line, restoring
  the full value when the reply ends. An exactly-spent deadline is an error
  rather than a zero re-arm, because a zero `SO_RCVTIMEO` means "block
  forever".

Pinned on both sides: `a_trickled_multi_line_reply_cannot_outrun_the_operation_timeout`
drives a clock-trickling peer under paused tokio time, and
`multi_line_reply_reads_share_one_deadline` reads back the timeouts the
blocking driver armed on the transcript and asserts they shrink within a reply
and reset between replies.

## PIPELINING

When the server advertises PIPELINING, `MAIL FROM` and `RCPT TO` commands are written in bounded recipient windows, with every reply in one window drained before the next is written. `DATA` is issued only after all recipient windows complete; the body is never in the pipelined batch. On RCPT failure mid-pipeline the transaction is reset before the body. The shared window bound respects the peer TCP window while preserving the original recipient indexes in `SendProgress`.

The window bracket is `Op::OpenReplyGroup` / `Op::CloseReplyGroup`, so both
drivers hold the stream `Broken` from a successful window write until the
complete reply group has drained, by construction rather than by mirroring.

Every direct-SMTP boundary is decorated with its `SmtpCommandPhase`, and that is
enforced by the type rather than by remembering it at each call site.
`DirectSmtp` builds its error exits only through the free `phased` helper,
whose return type is `PhasedError` - no `From<Error>` conversion and no
phase-less constructor - and the single `Step::Finish` that converts back to
`Error` is where the phase is stamped. A boundary added later cannot ship
undecorated; it will not build. Since the machine covers the pipelined and
non-pipelined paths alike, the guarantee now spans both.

Batch error translation also has one phase authority. `SmtpErrorContext` cannot
carry a command phase, so a batch site converting a low-level `Error` must use
the phase already attached to that value and cannot supply a second, disagreeing
phase. Per-recipient negative replies, which have a `Response` rather than an
`Error`, pass one explicit phase directly to the response classifier. This shape
is identical in the blocking and async drivers.

No SMTP-originated `AccountError` carries an `ErrorScope`, and that is
structural rather than conventional: `SmtpErrorContext` has no scope field and
one constructor, so no call site can attach one. `ErrorScope::Account` would
claim an account-wide fault for what is a single transaction on a single
connection, and the id-bearing variants take typed account-surface ids, none of
which represents an SMTP envelope address. Batch lanes correlate through their
`BatchItemId` and support-only envelope-recipient text instead. Every context
construction in the crate goes through `SmtpErrorContext::send`, and since the
send paths run on one core there is now exactly one set of those call sites for
both halves; both still pin the scope-less shape on the non-pipelined recipient
path with a paired transcript test.

That explicit argument is the part of the arrangement that is behaviour rather
than a type property, and it is pinned by a bare-550 recipient rejection: with
no enhanced status code the reply is classified by status alone, where the
`RcptTo` phase is what separates `NotFound(Mailbox)` from
`Authorization(PermissionDenied)`. Enhanced-code rejections such as `5.1.1`
classify identically for every phase, so a test built on one cannot pin this.

This covers normal negative replies as well as transport failures, which is the
case a per-call-site decoration missed: a server rejecting `MAIL FROM` is
`MailFrom`, a rejected recipient is `RcptTo`, a rejected `DATA` is
`DataCommand`, and a body-upload failure is `DataBody`. Those phases feed
`classify_response`, including the recipient-lane split, so an undecorated
rejection would make the PIPELINING path classify a plain relay rejection
differently from the non-pipelined path for the same wire exchange. Both halves
are pinned by transcript tests over all four boundaries.

A rejected `MAIL FROM` deliberately sends no `RSET`: it opened no transaction,
so there is nothing to reset and the connection stays reusable as it is. The
rejection paths that follow an accepted `MAIL FROM` go through
`reset_transaction`, which keeps the connection when the peer positively
acknowledges the reset and aborts it otherwise.

This is not a PIPELINING-only rule. The direct sends (`send_with_options` and
`send_bdat_with_options`) take the same shape when the server advertises no
PIPELINING: `MAIL FROM`, `RCPT TO` and `DATA` replies reach the core as
`OpOutcome::Reply`, so a routine 4xx/5xx is classified by the core instead of
arriving as a transport-shaped failure that would abort. Only genuine I/O or
parse failures abort. Both halves run one `DirectSmtp` machine for the
`MAIL FROM` + `RCPT TO` sequence, pipelined or not, and are pinned by
`unpipelined_server_rejections_keep_the_connection_reusable`, which asserts the
exact RSET traffic and that the connection is still unbroken afterwards. A
server rejection therefore costs no reconnect on either the pipelined or the
non-pipelined path.

## DSN and SendOptions

`SendOptions` is the per-message API for advanced ESMTP parameters:

- MAIL FROM params: `REQUIRETLS`, `FUTURERELEASE` (HOLDFOR / HOLDUNTIL), `DELIVERBY`, `MT-PRIORITY` (RFC 6710), DSN `RET`, DSN `ENVID`.
- Uniform RCPT TO params: DSN `NOTIFY`.
- Recipient-specific RCPT TO params: DSN `NOTIFY` overrides per recipient, `ORCPT` (default addr-type `rfc822`).

Server-advertised `DSN` is verified before DSN parameters are emitted. Recipient-specific params override matching global keywords (RFC 3461 §4.1 no-duplicate-keyword). Automatic DATA `SIZE` declarations reuse a message's existing final CRLF or account for the conditional CRLF the writer adds, and exclude the terminator line itself and transparency dots as RFC 1870 requires; BDAT uses its raw payload length.

Batch sends validate every recipient's RCPT parameters before `MAIL FROM`, in both sync and async paths. A local parameter error is an unsent `RcptTo` failure, never a silent downgrade to an unparameterized RCPT command.

Public entry points: `send_raw_with_options(...)` on SMTP and LMTP, sync and async.

## xtext encoding

RFC 3461 §4.1: any byte outside `%x21-%x7E`, plus `+`, plus `=`, is encoded as `+HH` with uppercase two-digit hex. UTF-8 multi-byte sequences are encoded byte-by-byte. Used for ENVID, ORCPT address part, and unknown ESMTP parameter values. Centralized in `util.rs`.

## VRFY / EXPN

`verify(addr, ...)` and `expand(list, ...)` on SMTP transports. Negative replies return as `Response`, not `Err`, because they are normal outcomes for these privacy-sensitive commands. Inputs are sanitized for CRLF and control characters before any wire write.

`MAIL FROM` and `RCPT TO` apply the same single-line control-character check
when their command values are constructed. This is a wire-boundary defense for
addresses created through the public unchecked constructor as well as parsed
addresses; hostile values fail before either command is written.

## BDAT

Opt-in via explicit `send_raw_bdat` / `send_raw_bdat_with_options`. Defaults still use DATA so dot-stuffing and BINARYMIME behavior are not changed silently. BDAT requires `CHUNKING` to be advertised.

## SIZE enforcement

If the server advertises `SIZE=<bytes>`, message size is checked client-side before MAIL FROM, and the advertised value is included as `SIZE=<bytes>` on the wire.

`SIZE 0` means "no declared maximum" per RFC 1870 section 3 and is stored as no limit. An unparseable limit is a hard `Parse` error on the EHLO reply rather than a silently dropped ceiling.

For DATA the declared size is `message.len()` when the buffer already ends in CRLF and `message.len() + 2` otherwise. The writer emits only `.\r\n` after an existing final CRLF; for an empty buffer, a missing final CRLF, or a buffer ending in a lone CR or LF, it first emits the required CRLF and then `.\r\n`. Thus the leading CRLF in the RFC 5321 terminator is the message's final CRLF, not an appended empty line. BDAT declares the raw chunk length because it has no terminator or transparency layer.

The chunked writer (`message_iter`, and the LMTP equivalent) makes the same decision without buffering the message: it tracks the last two bytes written across chunk boundaries, so a CRLF split across two iterator items is still recognized as the message's final CRLF.

A consequence: `Message::formatted()` is byte-identical to the delivered data section for any message ending in CRLF - delivery no longer appends an empty line the sender did not write. `body_raw()` still appends an unconditional CRLF before DKIM canonicalization, which is harmless in both directions because RFC 6376 simple and relaxed body canonicalization each strip trailing empty lines; signing and delivery still agree.

## Auth

`Credentials` is enum: `Password { username, password: Zeroizing<String> }` and `OAuth2 { identity, token_source: Arc<dyn TokenSource> }` (bifrost-net's trait). The OAuth token is read live from the shared source at each connect, so a token rotated on the source is presented on reconnect without rebuilding the transport. `Credentials` is `Clone` only - no `PartialEq`/`Eq`/serde derives (a live token source is neither comparable nor serializable; rotation material is the consumer's to persist). The `AUTH` command struct (`Auth`) likewise dropped those derives. The async transport reads the token via `oauth2_token().await`, so token sources may lock, join an in-flight refresh, or perform network refresh work without a polling shortcut; the blocking transport reads it via `oauth2_token_blocking()`, a single-poll of `current()` (a `StaticTokenSource` or already-fresh `OAuthRefresher` resolves immediately; a source needing a network refresh is rejected - live refresh requires the async transport).

Mechanisms (`Mechanism`, `#[non_exhaustive]`): PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, SCRAM-SHA-1, SCRAM-SHA-256, SCRAM-SHA-1-PLUS, SCRAM-SHA-256-PLUS. SCRAM is consumed from `bifrost-sasl` (the `-PLUS` suffix and token spelling have a single authority there); SMTP owns only the wire sequencing.

Password selection (`password_mechanism` in `authentication.rs`) chooses one mechanism from the advertised and allowed sets in the fixed order `SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > LOGIN`. A 535 is final and never walks a retry ladder, because descending from a bound mechanism to an unbound mechanism or PLAIN would be a silent security downgrade. RFC 5802 Section 6 downgrade protection drops the unbound `SCRAM-SHA-N` candidate when `SCRAM-SHA-N-PLUS` is advertised. A PLUS candidate is skipped only when no TLS peer certificate exists. If a certificate is present but its channel binding cannot be computed, the typed parse error propagates and authentication cannot silently fall through to PLAIN. The certificate-derived `tls-server-end-point` value is computed at most once per authentication and shared by either PLUS hash choice. SCRAM runs as a no-IR `334` challenge exchange driven by `ScramExchange`; `Mechanism::response` is never called for SCRAM.

OAuth credentials never use SCRAM: they pick the first advertised OAUTHBEARER/XOAUTH2 rung in caller order (`oauth_mechanism`) and run the stateless encoder. The connection's auth driver resolves the access token from the source once, up front, and threads it into `Auth::new` / `Mechanism::response_with_token`. The XOAUTH2 / OAUTHBEARER payload bytes are built by `bifrost-sasl` (`xoauth2_payload` / `oauthbearer_payload`, including the OAUTHBEARER GS2 identity escape); `response_with_token` only base64-frames them and owns the RFC 7628 `\x01` error-continuation. OAUTHBEARER before XOAUTH2 by default.

Stateless AUTH payloads remain `bifrost_sasl::Secret` values through the
command boundary. Base64 framing returns a `Zeroizing<String>`, and both sync
and async connections use zeroizing command buffers that are wiped before
reuse, immediately after each write attempt, and on drop. The wipe covers the
buffer the command was serialized into; it cannot cover an allocation the
serializing `write!` outgrew and replaced, so a longer-than-usual AUTH line can
still leave one stale heap copy behind. The `Auth`
representation is an enum whose start, initial
response, and continuation variants carry their formatting invariants
structurally, so formatting has no optional response to unwrap. LOGIN ignores
the server's human-readable prompt and answers its first and second challenges
with username and password respectively.

**Default behavior change (migration note).** `PASSWORD_MECHANISMS` now defaults to SCRAM (strongest first) then PLAIN, and LOGIN is no longer in it (opt-in legacy, mirroring IMAP's `allow_login = false`). A server advertising only `AUTH LOGIN` therefore yields an empty attempt order under the default and fails with "no compatible authentication mechanism" instead of silently downgrading to LOGIN. Callers that need LOGIN must pass it explicitly via `authentication(vec![Mechanism::Login, ..])`.

Plaintext AUTH is refused by default for both passwords and OAuth bearer tokens. Trusted local relays opt in with `dangerous_allow_insecure_auth(true)`.

AUTH continuation formatting treats challenge responses as continuation lines even for mechanisms supporting initial response. Required for OAUTHBEARER failed-auth dummy-cancel exchange (`AQ==` on the wire).

`SaslError` maps at the `From<SaslError> for Error` boundary: `Protocol` (malformed SASL, signature mismatch) to `ErrorKind::Parse`; `AuthFailed` (SCRAM `e=` server error) to `ErrorKind::InvalidInput` + `SmtpCommandPhase::Auth` so `account_error.rs` routes it to `Authorization(PolicyBlocked)`.

Builder helpers: `.password(user, password)` for password auth,
`.oauth2(identity, access_token)` for a raw OAuth bearer string, and
`.oauth2_source(identity, Arc<dyn TokenSource>)` for a shared rotation
source.

## LMTP

`LHLO` instead of EHLO. Default TCP port 24 (Postfix/Dovecot convention, not RFC-assigned).

Per-recipient response model: vector length equals envelope recipient count. RCPT-time rejection responses are preserved at the original recipient index; accepted recipients receive the post-DATA delivery response in original order.

Direct LMTP sends expose only that ordered response vector. Use the account-oriented batch send API when the caller needs each recipient's RCPT-versus-final-status phase and recovery classification.

Unix-domain LMTP constructors are `#[cfg(unix)]` on sync and Tokio. Unix sockets refuse STARTTLS explicitly.

## Native-tls only

The TLS backend matrix (rustls, boring-tls, rustls provider/verifier) has been removed. `CertificateStore::Default` always means the native-tls platform verifier.

`TlsParameters` carries the connector, SNI hostname, and a `dangerous_*` set for accepting invalid certs / hostnames (test only).

## Message builder

`Message::builder()` produces a builder for typed headers (From, To, Cc, Bcc, Reply-To, Subject, Date, Message-Id, In-Reply-To, References, MIME-Version, Content-Type, ...). `reply_to_many(...)` for RFC 5322 address-list `Reply-To`.

Display-name encoding emits RFC 5322 phrase text when the name is atom-shaped, RFC 2047 encoded-word otherwise. Avoids DKIM-breaking quoted-string rewrites at relays.

`MultiPart` kinds: `Mixed`, `Alternative`, `Related`, `Signed`, `Encrypted`, `Report { report_type }`. `Mixed` is the default for `MultiPart::builder().build()`.

Multipart builders ensure a `boundary` is present even when a caller supplies a boundary-less multipart `Content-Type`. `try_boundary`, `try_encrypted`, and `try_signed` are the fallible validation entry points; the infallible `boundary`, `MultiPart::encrypted`, and `MultiPart::signed` keep their signatures and panic on values that would break out of the MIME parameter (a deliberate runtime behavior change for callers passing unvalidated strings); default boundaries use OS randomness, and a caller-supplied boundary is regenerated if it appears at a MIME delimiter position in an added part.

A regeneration replaces the `boundary` PARAMETER and keeps every other
parameter on the multipart `Content-Type` verbatim, including ones outside this
crate's vocabulary. Rebuilding the header from `MultiPartKind` alone would
reproduce only the kind's own parameters, and since a re-roll is data-driven,
the same message would keep or lose a caller's vendor parameter depending on
its body. The fallback to a kind-rebuilt header remains for the one case a
lossless rebuild cannot cover: a parameter value containing a `"` or a `\`.

`SinglePartBuilder::body(String)` and `MessageBuilder::body(String)` infer `Content-Type: text/plain; charset=utf-8` when no content type is set.

`MessageBuilder::body` CRLF-normalizes bare LF at the builder boundary, so a byte body never reaches the DATA writer's defensive bare-LF dot-stuffing and gains a literal extra dot on a CRLF-strict relay. Normalization is scoped by the encoding that will actually be emitted: `String` input always normalizes, and `Vec<u8>` input normalizes only when it goes on the wire as literal `7bit`/`8bit` text. Byte input that is re-encoded (base64, `binary`) is opaque payload and is passed through untouched, so `0x0A` inside a binary attachment survives the round trip. `body_raw()` (DKIM) and the DATA writer both see the normalized buffer, so signing and delivery still agree. A pre-encoded `Body` is never rewritten.

One encoding decision governs both steps: the encoding chosen from the input
decides whether the buffer is normalized, and that same encoding is what the
resulting `Body` carries. Choosing again from the post-normalization bytes
would leave a window - narrow, and not reachable today - where a buffer was
CRLF-rewritten as text and then encoded as opaque payload, mutating bytes the
caller handed over untouchable. `crlf_normalization_never_changes_the_chosen_encoding`
pins the property the old two-decision shape rested on.

`Headers` cannot represent REPEATED header fields. `insert_raw` overwrites by
name, so a second `Received`, `Comments` or other repeated trace header
replaces the first rather than appending. That is correct for what this crate
is for - building fresh mail for submission - but a caller routing an existing
message through `Message` will lose repeated fields. Preserve such headers
outside `Headers`, or submit the original bytes with `send_raw`.

Typed headers for list management: `List-ID`, `List-Help`, `List-Unsubscribe`, `List-Unsubscribe-Post` (fixed value `List-Unsubscribe=One-Click` per RFC 8058), `List-Subscribe`, `List-Post`, `List-Owner`, `List-Archive`.

Content-type helpers: `text_plain_flowed()`, `text_plain_flowed_delsp()` (RFC 3676).

Batch builders: `MultiPart::multiparts(...)` and `.singleparts(...)`.

## DKIM

Default canonicalization is `relaxed/relaxed`. Signing keys: `From<rsa::RsaPrivateKey>` and `From<ed25519_dalek::SigningKey>` on `DkimSigningKey`. SHA-2 via `sha2` 0.10 to match `rsa` 0.9 digest traits.

Canonicalization follows RFC 6376 for empty bodies and missing final CRLFs. Relaxed headers are normalized iteratively, and `DKIM-Signature` is folded before `b=` before hashing so simple header canonicalization signs the exact emitted bytes. The signer intentionally does not support the `l=` body-length tag because it enables content-append attacks.

## Pool

`PoolConfig` configures min idle, max size, idle timeout, and the checkout NOOP probe. `max_size` bounds all live connections, checked out and idle, in both the blocking and async pools; checkout waits for a slot instead of dialing past the bound. Zero is rejected at checkout. Recycling refuses connections that are `Broken` and connections flagged for retirement by an LMTP final-status drain (see "Connection state and cancel-safety"). `min_idle` defaults to 0: no background pool worker is started, and expired connections are discarded at checkout. A positive `min_idle` enables the worker that expires and replenishes warm connections without exceeding `max_size`. `test_on_checkout(false)` opts out of the default NOOP probe for callers willing to retry one stale idle connection; it does not permit a connection already marked `Broken` to be reused. It buys nothing for LMTP transports, whose connections are retired at recycle and therefore never checked out idle.

Async `Drop` performs no spawn and no I/O. A healthy checked-out connection is
parked synchronously; a broken, retired, or shut-down connection is hard-dropped.
Recycling therefore has no await point and no blocking call at all, which is why
it is safe to run from `Drop`: there is no recycling future to be dropped before
its first poll, and no close to block on an unresponsive peer. Explicit
`shutdown()` owns graceful close work, runs closes concurrently, and each close
is bounded by the connection's operation timeout.

The admission gate participates in shutdown. Because `max_size` is enforced by a
semaphore (async) and a live counter plus condvar (blocking), a checkout blocked
on admission is reachable *only* through that gate: `shutdown()` closes the
semaphore and wakes the availability waiters on the async side, and notifies the
condvar on the blocking side. Blocking-side releases notify the condvar only
while holding the pool mutex, because the checkout's failed reserve and its wait
are atomic only against notifiers holding that mutex - a lock-free notify could
land between the two and strand the waiter. Async checkout additionally re-reads the pool state
after winning admission and before dialing. Without both halves of this a
checkout could park forever, or - once a connection checked out before shutdown
was returned and released its permit - dial a NEW connection through a pool that
was already closed.

## Bandwidth metering

`SmtpTransportBuilder::bandwidth_metering(sink, cap)` (and the async / LMTP
siblings) installs a `bifrost_net::MeterSinkHandle` and an
`Arc<AtomicU64>` cap on every connection the transport dials. Opt-in:
without it, `SmtpInfo::metering` is `WireMetering::disabled()` and both
socket funnels are unchanged. `UNLIMITED_BANDWIDTH` (`u64::MAX`) in the
atomic means no cap - the same encoding `bifrost-imap` uses, so ONE value
drives both halves of an IMAP-shaped account.

This exists because SMTP was the only protocol in the workspace whose
bytes reached the wire unmetered, which made `Account::set_bandwidth_cap`
silently PARTIAL rather than absent: `ImapAccount` implements it and
honours it on fetch traffic, but its `submission.rs` built an
`AsyncSmtpTransport` with no meter and no cap - so the cap was ignored on
exactly the upstream-heavy path a cap usually exists to protect.
`open_submission` now passes the account's own meter handle and cap
atomic, so one `set_bandwidth_cap` governs both halves.

Both socket funnels are metered: `AsyncNetworkStream`'s
`poll_read`/`poll_write` and `NetworkStream`'s blocking `Read`/`Write`.
The bucket in `client/metering.rs` RETURNS the debt it wants slept rather
than sleeping itself, which is the one design difference from IMAP's
`WireMetering`: SMTP has three call shapes over one bucket (an `async
fn`, a `poll_write` that cannot await, a blocking `Write`) and a
debt-returning bucket serves all three without a second implementation to
drift from the first. The async funnel parks the debt as a `Sleep` in the
stream so a throttled connection yields to the runtime; the blocking
funnel sleeps its calling thread, the semantic that caller already
accepted.

Two properties worth not regressing: tokens may go NEGATIVE, so a
transfer larger than one second of budget owes proportional time instead
of being clamped to one second (clamping would let a 1 B/s cap run at
hundreds of B/s), and the cap is re-read on every transfer, so a consumer
can retune or lift it without reconnecting. Bytes are charged as ACCEPTED
by the socket, not as offered, so a short write charges the remainder on
its retry.

Inbound and outbound are separate buckets: a large send must not throttle
the reply to it. In the async funnel the parked debt is separate too -
`throttle_in` and `throttle_out` are distinct `Sleep` slots, `poll_read`
consults only the first and `poll_write` only the second. A single shared
slot silently undid the separate buckets, because the debt parked by the
final DATA-body write gated the read of the server's reply to it, spending
the caller's per-operation read timeout on outbound debt.
`outbound_throttle_debt_does_not_gate_the_next_read` pins that a read
completes on its first poll while a write is still in debt.

The blocking funnel cannot deliver that property and is not claimed to.
`NetworkStream::charge` sleeps the calling thread, and that thread is the
same one that will issue the next read, so outbound debt necessarily
delays it. Overlapping the two directions there would need a second thread
or a non-blocking socket, neither of which belongs in a transport whose
contract is "this call blocks". For a caller who needs the guarantee under
a tight cap, the async half is the one that has it.

## Error model

`Error::kind()` returns `&ErrorKind`. SMTP reply failures are `Transient(Response)` or `Permanent(Response)`, so callers can inspect the full server reply through `smtp_response()`, `status()`, and `enhanced_status_code()`. Local buckets are `Parse`, `InvalidInput`, `FeatureUnsupported`, `ParameterOverLimit`, `Internal`, `Policy`, `Connection`, `Network`, `Timeout`, `Tls`, and `TransportShutdown`. `Policy` covers local refusals such as plaintext-AUTH refusal. `FeatureUnsupported` / `ParameterOverLimit` are the FUTURERELEASE discriminators (a relay that did not advertise the extension vs a HOLDFOR over the advertised max interval), kept distinct from generic `InvalidInput` so the account-error mapping can tell scheduled-send-unsupported apart from a malformed parameter. SMTP replies are line-bounded before allocation and decoded from bytes, so oversized or non-UTF-8 replies are `Parse`, not retryable network errors.

Internal pipeline errors carry two value-side decorations the classifier reads:

- `SmtpTransmissionState` (`Unsent` / `InFlight` / `Acknowledged`), attached via `with_attempt`.
- `SmtpCommandPhase`, attached via `with_phase`. The 16 variants cover every wire boundary the driver crosses: `Connect`, `Greeting`, `Hello`, `StartTls`, `Auth`, `MailFrom`, `RcptTo`, `DataCommand`, `DataBody`, `DataFinal`, `BdatBody`, `LmtpFinalStatus`, `Noop`, `Vrfy`, `Expn`, `Rset`. `DataCommand` covers the `DATA` command write/read; `DataBody` covers body upload; `DataFinal` covers the final reply after the dot terminator. Phase lives on `Error::Inner`, and every send-driver wire boundary attaches it before returning - transport failures and server rejections alike. On the pipelined path that is a compile-time property of `PhasedError` rather than a convention (see "PIPELINING"). Paired transcript tests assert the value directly on both I/O halves for the `MailFrom`, `RcptTo`, `DataCommand` and `DataBody` boundaries, so the default PIPELINING path cannot silently lose the classifier input.

`crates/smtp/src/transport/smtp/account_error.rs` is the single translation boundary into the shared `AccountError`. It funnels through `AccountErrorBuilder::try_build` (never the removed `.build()`), reads command phase only from `error.phase()`, and routes `InvalidInput` + `SmtpCommandPhase::Auth` (e.g. "no compatible authentication mechanism") to `Authorization(PolicyBlocked)` so consumers see a reauth/policy-change UX instead of `Request(Malformed) -> ClientBug` (internal telemetry). It also maps `FeatureUnsupported` to `Unsupported(AccountOperation::Send)` (so the IMAP layer surfaces a stable `Unsupported(Send)` kind when a relay lacks FUTURERELEASE) and `ParameterOverLimit` to `Request(Malformed)` (the hold time is outside the allowed window). `message_error_to_account_error(MessageError, Protocol) -> AccountError` is the boundary for builder-side validation failures (`MissingFrom`, `MissingTo`, `EmailMissingAt`, ...); every variant maps to `Request(Malformed)`.

Evidence must match what actually crossed the wire. A transport failure in the envelope phase (`MAIL FROM` / `RCPT TO`), including a failed write of a later PIPELINING recipient window, is `Unsent` and resolves through `SendProgress::mark_unresolved_unsent`: `DATA` has not been issued, so no message content can have reached the peer and an `Uncertain` lane would falsely claim a possible delivery. RCPT rejections the server already gave are preserved; accepted and unanswered recipients are both rewritten (an accepted RCPT with no `DATA` would otherwise resolve as a delivery that never happened), into `failed` lanes that `RecoveryClass` derives as `Retry(SameRequest)`. `InFlight` is reserved for failures from the `DATA` command onward.

`batch.rs` `SendProgress::resolve` is the per-recipient lane resolver. LMTP `DATA`-command negative replies route through `mark_accepted_rejected_with_response` so accepted recipients become per-recipient `Failed` lanes - never a batch-level `Err` (the previous shape let the engine resend the entire non-idempotent `Send` after the server rejected it). DATA-final-negative replies tag `SmtpCommandPhase::DataFinal`. LMTP `Accepted` at resolve time without a per-recipient `Final` is a programming bug caught by `debug_assert!` in debug builds and falls back to an `Uncertain` lane in release. Every failed and uncertain lane carries the envelope recipient as `DiagnosticText::support_only` so support exports preserve per-recipient correlation when N lanes share the same wire response text.

## Connection test harness

Two layers, and both still bite. The core-level tests in
`client/core/core_tests.rs` drive each machine with a scripted list of outcomes
and record its op stream, so window reply accounting, coalesced-reply handling,
RSET-and-keep, abort-on-I/O-failure, LMTP per-recipient final status and BDAT
sequencing are each pinned once, in the place the rule now lives, with no I/O
at all. Every one of them was confirmed to bite by reintroducing the bug it
describes. Below them, both drivers' transcript suites drive the full wire
exchange through their own adapter, which is what proves the adapters perform
the ops faithfully; the core tests cannot see a socket and so can never replace
them.

A scripted outcome list alone cannot reach every boundary. Writes and group
boundaries consume the next scripted failure whichever one it is, so a failure
meant for a later boundary is swallowed by the first non-reply op that runs -
which left `OpenReplyGroup`, `CloseReplyGroup`, `CloseLmtpDrain` and the
epilogue's own `RSET` write untargetable. The core harness therefore also takes
an *aimed* failure, `Harness::aiming(Aim)`, matched against the op stream rather
than queued in the script: `Aim::Position(n)` fails the nth op outright, and
`Aim::Kind(selector, nth)` fails the nth op matching a selector - an exact op
tag (`OPEN`, `CLOSE`, `DRAIN`, `READG`, `READ`, `BODY`, `BDAT`, `ABORT`) or a
`W:` prefix matched against the rendered write, so `Aim::Kind("W:RSET", 0)`
names the reset write and nothing else. The replies before the aimed op are
untouched, so the machine reaches the boundary in the state production would.
That is what pins the failed-RSET-write abort, the two pipelined group
boundaries returning their error without a second abort and with the recipient
lanes they had collected, and an LMTP drain close that fails after every final
status was recorded (the batch result stands; only the stream is lost).

The transcript suites carry the other half of that pair: a requested op is not
a performed op, so both suites observe the stream state after the exchange, not
only its result. `an_aborting_batch_failure_leaves_the_stream_broken` (abort,
and not retired), `a_rejected_pipelined_mail_from_restores_the_stream_at_the_group_close`
(the group bracket restores `Ok` when every reply drained),
`a_clean_lmtp_batch_drain_leaves_the_stream_broken_and_retired`
(`restore_ok: false` reaching the wire) and the direct-LMTP drain pins
(`Ok` plus retired) exist in both halves. Each was confirmed to bite by breaking
the adapter's handling of the op it targets - dropping the `Op::Abort` arm,
dropping the `Ok` restore in `finish_reply_group`, and ignoring `restore_ok` -
rather than only by reverting the core. One limit of that observable is worth
knowing before trusting it: asserting `has_broken()` after an abort does NOT
bite against removing `abort()`'s own `set_state(Broken)`, because the
shutdown it also performs sets `Closed`, which fails `verify()` just as well.
It is sharp only against the adapter dropping the op entirely, which is the
thing worth pinning.

`test_support::Transcript` is the in-process scripted peer both connection
drivers test against; it replaced the socket-listener tests, which were neither
hermetic nor deterministic. `test_support` itself now binds no `TcpListener`
and no `UnixListener`, spawns no threads and sleeps nowhere: the transport-level
LMTP delivery pins (`lmtp_transport_returns_per_recipient_statuses` and its
`tokio_` twin) run against a parked transcript connection, and the Unix-socket
case pins the routing decision - builder wiring plus the TLS-over-Unix refusal
raised inside `connection()` before any dial - since the kernel's socket is not
the part a test can prove in-process. No test anywhere in the crate - unit or
integration - binds a socket, spawns a listener thread or sleeps on the wall
clock. The pre-AUTH refusal ladder is the last thing that did: the post-greeting
authentication stage is factored into
`SmtpClient::authenticate_if_configured` (and its async twin) precisely so
`plaintext_auth_is_refused_before_auth_command` can drive it against a
transcript that scripts no AUTH step, where an AUTH command reaching the wire
fails the write instead of producing the policy error. The escape-hatch mirror
scripts the exact `AUTH PLAIN` line and the post-AUTH EHLO. A transcript is a
greeting plus an ordered list of
`expect(client_bytes, server_bytes)` steps. Three properties make it bite:

- Each client write must match one scripted step byte-for-byte, so command
  batching shape (which commands share a write) is pinned, not just command
  order.
- Server bytes become readable only after their step's write.
- Reads hand out at most one reply line. Without this, a `BufReader` prefetches
  a whole pipelined reply group, and a driver that skipped replies would still
  pass. Writing while replies are pending is an error, which is how the
  PIPELINING window drain and the LMTP surplus-final-reply desync are detected.
- `expect_coalesced(...)` opts one step out of the line split so its replies
  arrive as a single segment, the way a real peer's TCP stack coalesces
  adjacent replies. That is what a `BufReader` prefetches, so it drives the
  same surplus-detection path production has. The harness has no visibility
  below the buffer that a real transport lacks.

- `expect_then_close(...)` models a peer that answers with the given bytes and
  then hangs up. The bytes may be a truncated reply line, so this is the
  close-mid-response case: reads report EOF once the scripted bytes are gone
  and any later client write fails the way a write to a closed socket does.
  A truncated reply must surface as an `incomplete response` parse error,
  never a hang. The `MAX_RESPONSE_LINE_BYTES` cap is pinned the same way, by
  `an_oversized_greeting_line_is_a_parse_error_not_a_hang` in both halves. Note
  that the greeting it scripts is well-formed and merely over-long: a line of
  garbage fails to parse whether or not the cap is enforced, so it pins nothing
  about the cap.

The transcript stream carries no TLS, so `peer_certificate_der()` is `None`
under test. A test-only seam on `AsyncNetworkStream`
(`set_test_peer_certificate_der`, surfaced as
`AsyncSmtpConnection::from_transcript_with_peer_certificate`) injects a DER so
the connection-level channel-binding gate in `auth` is pinned hermetically in
both directions: a PLUS-advertising server with a usable certificate must be
answered with the PLUS mechanism, a present-but-unusable certificate is a hard
error with no fallback AUTH written, and only an absent certificate falls
through to a weaker mechanism.

`expect_then_stall` and `Transcript::silent()` model a peer that accepts and
then never answers; reads park with no waker, so only the caller's own timeout
or cancellation resumes the task. That is what the async timeout, setup-deadline
and cancellation tests observe, under `start_paused` tokio time.

`test_support::StalledPeer` is the same idea one layer lower: a bare
`AsyncRead`/`AsyncWrite` that swallows every byte and parks every read. It
exists for the setup-deadline cases whose traffic is not scriptable - a TLS
`ClientHello` is opaque bytes, so `expect(...)` cannot match it - and it is what
`tokio_tls_handshake_uses_deadline` drives `upgrade_tls_stream` against, in
place of the listener plus sleeping thread that test used to need.

Pool-level tests park a transcript-backed connection directly
(`Pool::park_for_test`) and drive the public batch entry points through it, so
the LMTP retirement rule and SMTP connection reuse are pinned end to end
without dialing anything. A real TLS handshake stays out of scope: the
`starttls` transcripts follow the upgrade to the handshake boundary (command
written, positive reply read) and stop there, since the in-process stream is
not a TCP socket.

## Example validation

Examples live under `crates/smtp/examples`. Sync examples require no crate
features. Tokio examples are gated by `tokio`. Validate examples with the SMTP
crate checks under both the default feature set and `tokio`.

## Module layout

```
crates/smtp/src/
├── message/             - builder, headers, MIME parts, address parsers
├── transport/
│   └── smtp/
│       ├── client/      - core (sans-I/O protocol machines), connection and
│       │                  async_connection (the two I/O adapters), net,
│       │                  async_net, metering, tls
│       ├── transport.rs / async_transport.rs - public sync + async transports
│       ├── commands.rs  - EHLO, MAIL, RCPT, DATA, BDAT, AUTH, NOOP, RSET, VRFY, EXPN, STARTTLS, LHLO
│       ├── extension.rs - ServerInfo, SendOptions, MAIL/RCPT parameters
│       ├── response.rs  - parser, enhanced status codes
│       └── test_support.rs - in-process `Transcript` harness + mock servers
└── error.rs             - Error, ErrorKind
```

## SMTP-specific code style

- `client::` is private; low-level connection/network stream types are crate-internal.
- TLS uses native-tls unconditionally; rustls/boring features have been removed.
- `#[non_exhaustive]` on `Error`, `ErrorKind`, DKIM enums, `CertificateStore`.
- `AsyncTransport` requires `Sync` so borrowed methods return `Send` futures.
- Async traits use native `impl Future + Send` return types; no `async-trait`.
