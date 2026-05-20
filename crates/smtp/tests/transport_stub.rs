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
}
