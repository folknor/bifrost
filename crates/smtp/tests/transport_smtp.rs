#[cfg(test)]
#[cfg(all(feature = "smtp-transport", feature = "builder"))]
mod sync {
    use bifrost_smtp::{Message, SmtpTransport, Transport};

    #[test]
    #[ignore = "requires a local SMTP server on 127.0.0.1:2525"]
    fn smtp_transport_simple() {
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        let sender = SmtpTransport::builder_dangerous("127.0.0.1")
            .port(2525)
            .build();
        sender.send(&email).unwrap();
    }
}

#[cfg(test)]
#[cfg(all(feature = "smtp-transport", feature = "builder", feature = "tokio"))]
mod tokio {
    use bifrost_smtp::{AsyncSmtpTransport, AsyncTransport, Message, TokioExecutor};

    #[tokio::test]
    #[ignore = "requires a local SMTP server on 127.0.0.1:2525"]
    async fn smtp_transport_simple_tokio() {
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap();

        let sender: AsyncSmtpTransport<TokioExecutor> =
            AsyncSmtpTransport::<TokioExecutor>::builder_dangerous("127.0.0.1")
                .port(2525)
                .build();
        sender.send(&email).await.unwrap();
    }
}

#[cfg(test)]
#[cfg(all(feature = "smtp-transport", feature = "tokio"))]
mod read_response_caps {
    use std::{io::Write, net::TcpListener, thread, time::Duration};

    use bifrost_smtp::{AsyncSmtpTransport, TokioExecutor};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_returns_on_oversized_banner_line() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut line = vec![b'x'; 4096];
                line.extend_from_slice(b"\r\n");
                let _ = sock.write_all(&line);
            }
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            AsyncSmtpTransport::<TokioExecutor>::builder_dangerous("127.0.0.1")
                .port(addr.port())
                .build::<TokioExecutor>()
                .test_connection(),
        )
        .await
        .expect("connect must return within 5s, not hang");

        let err = match result {
            Ok(_) => panic!("oversized line must surface as an error"),
            Err(e) => e,
        };
        assert!(
            err.is_response(),
            "expected response-kind error, got {err:?}"
        );
    }
}
