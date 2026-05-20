use bifrost_smtp::{
    AsyncSmtpTransport, AsyncTransport, Message, TokioExecutor, message::header::ContentType,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let email = Message::builder()
        .from("NoBody <nobody@domain.tld>".parse().unwrap())
        .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
        .to("Hei <hei@domain.tld>".parse().unwrap())
        .subject("Happy new async year")
        .header(ContentType::TEXT_PLAIN)
        .body(String::from("Be happy with async!"))
        .unwrap();

    // Open a remote connection to gmail
    let mailer: AsyncSmtpTransport<TokioExecutor> =
        AsyncSmtpTransport::<TokioExecutor>::relay("smtp.gmail.com")
            .unwrap()
            .password("smtp_username", "smtp_password")
            .build();

    // Send the email
    match mailer.send(&email).await {
        Ok(_) => println!("Email sent successfully!"),
        Err(e) => panic!("Could not send email: {e:?}"),
    }
}
