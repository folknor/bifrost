# bifrost-smtp

SMTP client crate for the bifrost workspace.

This crate currently descends from `lettre` 0.11.22 and keeps the same broad
feature surface while the bifrost SMTP implementation is split out and adapted.
See the workspace `NOTICE` file for upstream attribution.

```rust,no_run
use bifrost_smtp::message::{Mailbox, header::ContentType};
use bifrost_smtp::{Message, SmtpTransport, Transport};

fn main() {
    let email = Message::builder()
        .from(Mailbox::new(Some("NoBody".to_owned()), "nobody@domain.tld".parse().unwrap()))
        .reply_to(Mailbox::new(Some("Yuin".to_owned()), "yuin@domain.tld".parse().unwrap()))
        .to(Mailbox::new(Some("Hei".to_owned()), "hei@domain.tld".parse().unwrap()))
        .subject("Happy new year")
        .header(ContentType::TEXT_PLAIN)
        .body(String::from("Be happy!"))
        .unwrap();

    let mailer = SmtpTransport::relay("smtp.example.com")
        .unwrap()
        .password("smtp_username", "smtp_password")
        .build();

    mailer.send(&email).unwrap();
}
```

OAuth 2.0 access tokens can be used with `.oauth2(identity, access_token)`.
That configures `OAUTHBEARER` and `XOAUTH2`, in that preference order; the
transport uses the first configured mechanism advertised by the server.
Passwords and bearer tokens are refused on plaintext SMTP connections by
default. Trusted local test relays can opt in with
`.dangerous_allow_insecure_auth(true)`.
