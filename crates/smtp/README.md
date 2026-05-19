# bifrost-smtp

SMTP client crate for the bifrost workspace.

This crate currently descends from `lettre` 0.11.22 and keeps the same broad
feature surface while the bifrost SMTP implementation is split out and adapted.
See the workspace `NOTICE` file for upstream attribution.

```rust,no_run
use bifrost_smtp::message::{Mailbox, header::ContentType};
use bifrost_smtp::transport::smtp::authentication::Credentials;
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

    let creds = Credentials::new("smtp_username".to_owned(), "smtp_password".to_owned());

    let mailer = SmtpTransport::relay("smtp.example.com")
        .unwrap()
        .credentials(creds)
        .build();

    mailer.send(&email).unwrap();
}
```
