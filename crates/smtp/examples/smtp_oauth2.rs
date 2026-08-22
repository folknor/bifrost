use bifrost_smtp::{Message, SmtpTransport, Transport, message::header::ContentType};

fn main() {
    tracing_subscriber::fmt::init();

    let email = Message::builder()
        .from("NoBody <nobody@domain.tld>".parse().unwrap())
        .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
        .to("Hei <hei@domain.tld>".parse().unwrap())
        .subject("OAuth2 SMTP example")
        .header(ContentType::TEXT_PLAIN)
        .body(String::from(
            "This message authenticates with an OAuth2 access token.",
        ))
        .unwrap();

    let mailer = SmtpTransport::starttls_relay("smtp.example.com")
        .unwrap()
        .oauth2("user@example.com", "oauth2_access_token")
        .build();

    match mailer.send(&email) {
        Ok(_) => println!("Email sent successfully!"),
        Err(error) => panic!("Could not send email: {error:?}"),
    }
}
