#[cfg(all(feature = "file-transport", feature = "builder"))]
fn default_date() -> std::time::SystemTime {
    use std::time::{Duration, SystemTime};

    // Tue, 15 Nov 1994 08:12:31 GMT
    SystemTime::UNIX_EPOCH + Duration::from_secs(784887151)
}

#[cfg(test)]
#[cfg(all(feature = "file-transport", feature = "builder"))]
mod sync {
    use std::{
        env::temp_dir,
        fs::{read_to_string, remove_file},
    };

    use bifrost_smtp::{FileTransport, Message, Transport};

    use crate::default_date;

    #[test]
    fn file_transport() {
        let sender = FileTransport::new(temp_dir());
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .date(default_date())
            .body(String::from("Be happy!"))
            .unwrap();

        let result = sender.send(&email);
        let id = result.unwrap();

        let eml_file = temp_dir().join(format!("{id}.eml"));
        let eml = read_to_string(&eml_file).unwrap();

        assert_eq!(
            eml,
            concat!(
                "From: NoBody <nobody@domain.tld>\r\n",
                "Reply-To: Yuin <yuin@domain.tld>\r\n",
                "To: Hei <hei@domain.tld>\r\n",
                "Subject: Happy new year\r\n",
                "Date: Tue, 15 Nov 1994 08:12:31 +0000\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Be happy!"
            )
        );
        remove_file(eml_file).unwrap();
    }

    #[test]
    #[cfg(feature = "file-transport-envelope")]
    fn file_transport_with_envelope() {
        let sender = FileTransport::with_envelope(temp_dir());
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .date(default_date())
            .body(String::from("Be happy!"))
            .unwrap();

        let result = sender.send(&email);
        let id = result.unwrap();

        let eml_file = temp_dir().join(format!("{id}.eml"));
        let eml = read_to_string(&eml_file).unwrap();

        let json_file = temp_dir().join(format!("{id}.json"));
        let json = read_to_string(&json_file).unwrap();

        assert_eq!(
            eml,
            concat!(
                "From: NoBody <nobody@domain.tld>\r\n",
                "Reply-To: Yuin <yuin@domain.tld>\r\n",
                "To: Hei <hei@domain.tld>\r\n",
                "Subject: Happy new year\r\n",
                "Date: Tue, 15 Nov 1994 08:12:31 +0000\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Be happy!"
            )
        );

        assert_eq!(
            json,
            "{\"forward_path\":[\"hei@domain.tld\"],\"reverse_path\":\"nobody@domain.tld\"}"
        );

        let (e, m) = sender.read(&id).unwrap();

        assert_eq!(&e, email.envelope());
        assert_eq!(m, email.formatted());

        remove_file(eml_file).unwrap();
        remove_file(json_file).unwrap();
    }
}

#[cfg(test)]
#[cfg(all(feature = "file-transport", feature = "builder", feature = "tokio"))]
mod tokio {
    use std::{
        env::temp_dir,
        fs::{read_to_string, remove_file},
    };

    use bifrost_smtp::{AsyncFileTransport, AsyncTransport, Message, TokioExecutor};

    use crate::default_date;

    #[tokio::test]
    async fn file_transport_tokio() {
        let sender = AsyncFileTransport::<TokioExecutor>::new(temp_dir());
        let email = Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .date(default_date())
            .body(String::from("Be happy!"))
            .unwrap();

        let result = sender.send(&email).await;
        let id = result.unwrap();

        let eml_file = temp_dir().join(format!("{id}.eml"));
        let eml = read_to_string(&eml_file).unwrap();

        assert_eq!(
            eml,
            concat!(
                "From: NoBody <nobody@domain.tld>\r\n",
                "Reply-To: Yuin <yuin@domain.tld>\r\n",
                "To: Hei <hei@domain.tld>\r\n",
                "Subject: Happy new year\r\n",
                "Date: Tue, 15 Nov 1994 08:12:31 +0000\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "Be happy!"
            )
        );
        remove_file(eml_file).unwrap();
    }
}
