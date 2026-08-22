#[cfg(test)]
mod tokio {
    use std::sync::Arc;

    use bifrost_smtp::{
        AsyncTransport, BoxedAsyncTransport, Message,
        transport::stub::{AsyncStubTransport, Error as StubError},
    };

    #[tokio::test]
    async fn stub_transport_tokio() {
        let sender_ok = AsyncStubTransport::new_ok();
        let sender_ko = AsyncStubTransport::new_error();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        sender_ok.send(&email).await.unwrap();
        sender_ko.send(&email).await.unwrap_err();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(sender_ok.messages().await, expected_messages);
    }

    #[tokio::test]
    async fn boxed_stub_transport_tokio() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BoxedAsyncTransport<(), StubError>>();

        let sender = AsyncStubTransport::new_ok();
        let observer = sender.clone();
        let boxed: BoxedAsyncTransport<(), StubError> = BoxedAsyncTransport::new(sender);
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Boxed")
            .body(String::from("boxed body"))
            .unwrap();

        boxed.send(&email).await.unwrap();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(observer.messages().await, expected_messages);
    }

    #[tokio::test]
    async fn arc_stub_transport_tokio() {
        let sender = Arc::new(AsyncStubTransport::new_ok());
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Arc")
            .body(String::from("arc body"))
            .unwrap();

        sender.send(&email).await.unwrap();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(sender.messages().await, expected_messages);
    }

    #[tokio::test]
    async fn bcc_stays_in_the_envelope_and_out_of_the_payload() {
        let sender = AsyncStubTransport::new_ok();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .bcc("hidden@domain.tld".parse().unwrap())
            .subject("blind copy")
            .body(String::from("body"))
            .unwrap();

        sender.send(&email).await.unwrap();

        let messages = sender.messages().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0.to().len(), 2);
        assert!(
            messages[0]
                .0
                .to()
                .iter()
                .any(|address| address.to_string() == "hidden@domain.tld")
        );
        assert!(!messages[0].1.contains("Bcc:"));
    }

    #[tokio::test]
    async fn configured_failure_still_records_the_message() {
        let sender = AsyncStubTransport::new_error();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .body(String::from("body"))
            .unwrap();

        sender.send(&email).await.unwrap_err();

        assert_eq!(sender.messages().await.len(), 1);
    }

    #[tokio::test]
    async fn raw_invalid_utf8_is_recorded_lossily() {
        let sender = AsyncStubTransport::new_ok();
        let envelope = bifrost_smtp::address::Envelope::new(
            Some("nobody@domain.tld".parse().unwrap()),
            vec!["hei@domain.tld".parse().unwrap()],
        )
        .unwrap();

        sender.send_raw(&envelope, b"\xff\xfe").await.unwrap();

        assert_eq!(sender.messages().await[0].1, "\u{fffd}\u{fffd}");
    }
}
