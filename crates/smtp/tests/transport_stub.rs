#[cfg(test)]
mod sync {
    use std::sync::Arc;

    use bifrost_smtp::{
        BoxedTransport, Message, Transport,
        transport::stub::{Error as StubError, StubTransport},
    };

    #[test]
    fn stub_transport() {
        let sender_ok = StubTransport::new_ok();
        let sender_ko = StubTransport::new_error();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        sender_ok.send(&email).unwrap();
        sender_ko.send(&email).unwrap_err();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(sender_ok.messages(), expected_messages);
    }

    #[test]
    fn boxed_stub_transport() {
        let sender = StubTransport::new_ok();
        let observer = sender.clone();
        let boxed: BoxedTransport<(), StubError> = Box::new(sender);
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Boxed")
            .body(String::from("boxed body"))
            .unwrap();

        boxed.send(&email).unwrap();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(observer.messages(), expected_messages);
    }

    #[test]
    fn stub_transport_records_bcc_in_the_envelope_but_not_in_the_headers() {
        // `Transport::send` serializes with `Message::formatted`, which runs
        // after the builder has stripped `Bcc` into the envelope. The recorded
        // pair is the exact split the wire sees: the recipient is addressed by
        // RCPT TO but is invisible in the DATA payload.
        let sender = StubTransport::new_ok();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .bcc("hidden@domain.tld".parse().unwrap())
            .subject("blind copy")
            .body(String::from("body"))
            .unwrap();

        sender.send(&email).unwrap();

        let messages = sender.messages();
        assert_eq!(messages.len(), 1);
        let (envelope, serialized) = &messages[0];
        assert_eq!(envelope.to().len(), 2);
        assert!(
            envelope
                .to()
                .iter()
                .any(|address| address.to_string() == "hidden@domain.tld"),
            "envelope must keep the Bcc recipient: {envelope:?}"
        );
        assert!(
            !serialized.contains("Bcc:"),
            "Bcc header must not reach the wire: {serialized}"
        );
    }

    #[test]
    fn stub_transport_records_the_message_even_when_it_reports_failure() {
        // The log is written before the configured response is returned, so a
        // failing stub still exposes what would have been transmitted.
        let sender = StubTransport::new_error();
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Failure")
            .body(String::from("body"))
            .unwrap();

        sender.send(&email).unwrap_err();

        assert_eq!(sender.messages().len(), 1);
    }

    #[test]
    fn stub_transport_replaces_invalid_utf8_in_the_recorded_payload() {
        // `send_raw` records via `String::from_utf8_lossy`, so a binary
        // payload is not round-trippable out of the log. Anything asserting on
        // exact bytes has to go through `send_raw` inputs, not `messages()`.
        let sender = StubTransport::new_ok();
        let envelope = bifrost_smtp::address::Envelope::new(
            Some("nobody@domain.tld".parse().unwrap()),
            vec!["hei@domain.tld".parse().unwrap()],
        )
        .unwrap();

        sender.send_raw(&envelope, b"\xff\xfe").unwrap();

        assert_eq!(sender.messages()[0].1, "\u{fffd}\u{fffd}");
    }

    #[test]
    fn arc_stub_transport() {
        let sender = Arc::new(StubTransport::new_ok());
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Arc")
            .body(String::from("arc body"))
            .unwrap();

        sender.send(&email).unwrap();

        let expected_messages = [(
            email.envelope().clone(),
            String::from_utf8(email.formatted()).unwrap(),
        )];
        assert_eq!(sender.messages(), expected_messages);
    }
}

#[cfg(test)]
#[cfg(feature = "tokio")]
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
