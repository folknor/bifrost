# Bifrost SMTP Examples

This folder contains examples showing how to use Bifrost SMTP in your own projects.
The crate is async-only and Tokio is not optional, so every example builds
under the default feature set. TLS is provided by native-tls, and SMTP/LMTP
transports always use connection pooling.

## Message builder examples

- [basic_html.rs] - Create an HTML email.

## Tokio SMTP examples

- [tokio_smtp_tls.rs] - Async TLS-wrapped SMTP submission.
- [tokio_smtp_starttls.rs] - Async STARTTLS SMTP submission.

The examples use placeholder relay names and credentials. Replace them with
values from your SMTP provider or local mail server before running them.
For error handling, SMTP reply failures carry the full server response through
`transport::smtp::Error::smtp_response()`, with helpers for the status code and
enhanced status code.

[basic_html.rs]: ./basic_html.rs
[tokio_smtp_tls.rs]: ./tokio_smtp_tls.rs
[tokio_smtp_starttls.rs]: ./tokio_smtp_starttls.rs
