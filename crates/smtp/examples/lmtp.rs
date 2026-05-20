use bifrost_smtp::{LmtpTransport, Message, Transport};

fn main() {
    tracing_subscriber::fmt::init();

    let email = Message::builder()
        .from("NoBody <nobody@domain.tld>".parse().unwrap())
        .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
        .to("Hei <hei@domain.tld>".parse().unwrap())
        .to("Idk <idk@domain.tld>".parse().unwrap())
        .subject("Happy new year")
        .body(String::from("Be happy!"))
        .unwrap();

    // Open a local LMTP TCP connection on port 24.
    let mailer = LmtpTransport::builder_dangerous("localhost").build();

    match mailer.send(&email) {
        Ok(responses) => println!("Email accepted with per-recipient statuses: {responses:?}"),
        Err(error) => panic!("Could not send email: {error:?}"),
    }
}
