# Bifrost SMTP Examples

This folder contains examples showing how to use Bifrost SMTP in your own projects.
The sync examples need no crate features. The async examples require the
`tokio` feature. TLS is always provided by native-tls, and SMTP/LMTP transports
always use connection pooling.

## Message builder examples

- [basic_html.rs] - Create an HTML email.

## SMTP examples

- [smtp.rs] - Send through a local SMTP daemon on port 25.
- [smtp_tls.rs] - Send through a TLS-wrapped SMTP submission server with password auth.
- [smtp_starttls.rs] - Send through a STARTTLS SMTP submission server with password auth.
- [smtp_oauth2.rs] - Send through a STARTTLS SMTP submission server with OAuth2 bearer-token auth.
- [smtp_selfsigned.rs] - Send through a TLS-wrapped SMTP server with a custom root certificate.
- [autoconfigure.rs] - Probe TLS-wrapped, STARTTLS, and plaintext connection modes for one host.

## Async Tokio examples

- [tokio_smtp_tls.rs] - Async TLS-wrapped SMTP submission.
- [tokio_smtp_starttls.rs] - Async STARTTLS SMTP submission.

## LMTP examples

- [lmtp.rs] - Send through a local LMTP TCP listener on port 24 and inspect one response per recipient.

The examples use placeholder relay names and credentials. Replace them with
values from your SMTP provider or local mail server before running them.
For error handling, SMTP reply failures carry the full server response through
`transport::smtp::Error::smtp_response()`, with helpers for the status code and
enhanced status code.

[basic_html.rs]: ./basic_html.rs
[smtp.rs]: ./smtp.rs
[smtp_tls.rs]: ./smtp_tls.rs
[smtp_starttls.rs]: ./smtp_starttls.rs
[smtp_oauth2.rs]: ./smtp_oauth2.rs
[smtp_selfsigned.rs]: ./smtp_selfsigned.rs
[autoconfigure.rs]: ./autoconfigure.rs
[tokio_smtp_tls.rs]: ./tokio_smtp_tls.rs
[tokio_smtp_starttls.rs]: ./tokio_smtp_starttls.rs
[lmtp.rs]: ./lmtp.rs
