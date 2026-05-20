# SASL state machine and channel binding

bifrost-smtp's AUTH still hand-rolls PLAIN, LOGIN, XOAUTH2, and
OAUTHBEARER. bifrost-imap reportedly has SCRAM-PLUS policy work (per
commit f1760f3). Two open questions before adding SCRAM-SHA-*-PLUS
anywhere.

SASL library choice. Does IMAP use rsasl, a SCRAM-only crate, or a
hand-rolled implementation? If IMAP is on rsasl, SMTP joining is the
cheap move. If IMAP is hand-rolled, a private `bifrost-sasl` mini-module
shared between the two crates avoids two divergent stacks. Either way,
rsasl should not shape the public API surface, per the existing SMTP
plan note.

Channel-binding plumbing is independent of the SASL choice.
SCRAM-SHA-*-PLUS needs `tls-server-end-point` (TLS 1.3 killed
`tls-unique`), which is a SHA-256 of the server certificate's
SubjectPublicKeyInfo per RFC 5929. native-tls's
`TlsStream::peer_certificate()` provides the cert; bifrost's
`NetworkStream` and `AsyncNetworkStream` need to expose it upward to
whatever drives SASL. That plumbing is owed regardless of which SASL
stack wins.

Open: what does IMAP do today, and is ratatoskr asking for SCRAM-PLUS
on both protocols or just IMAP?
