#[cfg(all(feature = "sendmail-transport", feature = "builder"))]
mod support {
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
    };

    use bifrost_smtp::Message;

    pub struct FakeSendmail {
        command: PathBuf,
        args: PathBuf,
        input: PathBuf,
    }

    impl FakeSendmail {
        pub fn success(label: &str) -> Self {
            Self::new(label, 0, "")
        }

        pub fn failure(label: &str, stderr: &str) -> Self {
            Self::new(label, 72, stderr)
        }

        pub fn failure_without_stderr(label: &str) -> Self {
            Self::new(label, 72, "")
        }

        pub fn command(&self) -> OsString {
            self.command.clone().into_os_string()
        }

        pub fn args(&self) -> String {
            fs::read_to_string(&self.args).unwrap()
        }

        pub fn input(&self) -> String {
            fs::read_to_string(&self.input).unwrap()
        }

        fn new(label: &str, status: i32, stderr: &str) -> Self {
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("sendmail-tests")
                .join(format!("{label}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();

            let command = root.join(command_file_name());
            let args = root.join("args.txt");
            let input = root.join("input.eml");
            let stderr_file = root.join("stderr.txt");
            fs::write(&stderr_file, stderr).unwrap();

            write_command(&command, &args, &input, &stderr_file, status);

            Self {
                command,
                args,
                input,
            }
        }
    }

    pub fn email() -> Message {
        Message::builder()
            .from("NoBody <nobody@domain.tld>".parse().unwrap())
            .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
            .to("Hei <hei@domain.tld>".parse().unwrap())
            .subject("Happy new year")
            .body(String::from("Be happy!"))
            .unwrap()
    }

    pub fn assert_delivered(fake: &FakeSendmail, email: &Message) {
        let args = fake.args();
        assert!(args.lines().any(|arg| arg == "-i"), "{args:?}");
        assert!(args.lines().any(|arg| arg == "-f"), "{args:?}");
        assert!(
            args.lines().any(|arg| arg == "nobody@domain.tld"),
            "{args:?}"
        );
        assert!(args.lines().any(|arg| arg == "--"), "{args:?}");
        assert!(args.lines().any(|arg| arg == "hei@domain.tld"), "{args:?}");
        assert_eq!(fake.input().into_bytes(), email.formatted());
    }

    #[cfg(unix)]
    fn command_file_name() -> &'static str {
        "sendmail"
    }

    #[cfg(windows)]
    fn command_file_name() -> &'static str {
        "sendmail.cmd"
    }

    #[cfg(unix)]
    fn write_command(command: &Path, args: &Path, input: &Path, stderr_file: &Path, status: i32) {
        use std::os::unix::fs::PermissionsExt;

        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncat > {}\ncat {} >&2\nexit {status}\n",
            shell_quote(args.display().to_string()),
            shell_quote(input.display().to_string()),
            shell_quote(stderr_file.display().to_string())
        );
        fs::write(command, script).unwrap();
        let mut permissions = fs::metadata(command).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(command, permissions).unwrap();
    }

    #[cfg(windows)]
    fn write_command(command: &Path, args: &Path, input: &Path, stderr_file: &Path, status: i32) {
        let script = format!(
            "@echo off\r\n(for %%A in (%*) do echo %%~A) > \"{}\"\r\nmore > \"{}\"\r\ntype \"{}\" 1>&2\r\nexit /b {status}\r\n",
            args.display(),
            input.display(),
            stderr_file.display()
        );
        fs::write(command, script).unwrap();
    }

    #[cfg(unix)]
    fn shell_quote(value: impl AsRef<str>) -> String {
        format!("'{}'", value.as_ref().replace('\'', "'\\''"))
    }
}

#[cfg(test)]
#[cfg(all(feature = "sendmail-transport", feature = "builder"))]
mod sync {
    use bifrost_smtp::{SendmailTransport, Transport};

    use crate::support::{FakeSendmail, assert_delivered, email};

    #[test]
    fn sendmail_transport() {
        let fake = FakeSendmail::success("sync-success");
        let sender = SendmailTransport::new_with_command(fake.command());
        let email = email();

        let result = sender.send(&email);
        println!("{result:?}");
        assert!(result.is_ok());
        assert_delivered(&fake, &email);
    }

    #[test]
    fn sendmail_transport_reports_stderr_on_failure() {
        let fake = FakeSendmail::failure("sync-failure", "synthetic sendmail failure");
        let sender = SendmailTransport::new_with_command(fake.command());
        let email = email();

        let error = sender.send(&email).unwrap_err();
        assert!(error.is_client());
        assert!(
            error.to_string().contains("synthetic sendmail failure"),
            "{error:?}"
        );
    }

    #[test]
    fn sendmail_transport_reports_failure_without_stderr() {
        let fake = FakeSendmail::failure_without_stderr("sync-failure-empty-stderr");
        let sender = SendmailTransport::new_with_command(fake.command());
        let email = email();

        let error = sender.send(&email).unwrap_err();
        assert!(error.is_client());
    }
}

#[cfg(test)]
#[cfg(all(
    feature = "sendmail-transport",
    feature = "builder",
    feature = "tokio1"
))]
mod tokio_1 {
    use bifrost_smtp::{AsyncSendmailTransport, AsyncTransport, Tokio1Executor};
    use tokio1_crate as tokio;

    use crate::support::{FakeSendmail, assert_delivered, email};

    #[tokio::test]
    async fn sendmail_transport_tokio1() {
        let fake = FakeSendmail::success("tokio-success");
        let sender = AsyncSendmailTransport::<Tokio1Executor>::new_with_command(fake.command());
        let email = email();

        let result = sender.send(&email).await;
        println!("{result:?}");
        assert!(result.is_ok());
        assert_delivered(&fake, &email);
    }

    #[tokio::test]
    async fn sendmail_transport_tokio1_reports_stderr_on_failure() {
        let fake = FakeSendmail::failure("tokio-failure", "synthetic tokio sendmail failure");
        let sender = AsyncSendmailTransport::<Tokio1Executor>::new_with_command(fake.command());
        let email = email();

        let error = sender.send(&email).await.unwrap_err();
        assert!(error.is_client());
        assert!(
            error
                .to_string()
                .contains("synthetic tokio sendmail failure"),
            "{error:?}"
        );
    }
}

#[cfg(test)]
#[cfg(all(
    feature = "sendmail-transport",
    feature = "builder",
    feature = "async-std1"
))]
mod asyncstd_1 {
    use bifrost_smtp::{AsyncSendmailTransport, AsyncStd1Executor, AsyncTransport};

    use crate::support::{FakeSendmail, assert_delivered, email};

    #[async_std::test]
    async fn sendmail_transport_asyncstd1() {
        let fake = FakeSendmail::success("asyncstd-success");
        let sender = AsyncSendmailTransport::<AsyncStd1Executor>::new_with_command(fake.command());
        let email = email();

        let result = sender.send(&email).await;
        println!("{result:?}");
        assert!(result.is_ok());
        assert_delivered(&fake, &email);
    }

    #[async_std::test]
    async fn sendmail_transport_asyncstd1_reports_stderr_on_failure() {
        let fake = FakeSendmail::failure("asyncstd-failure", "synthetic asyncstd sendmail failure");
        let sender = AsyncSendmailTransport::<AsyncStd1Executor>::new_with_command(fake.command());
        let email = email();

        let error = sender.send(&email).await.unwrap_err();
        assert!(error.is_client());
        assert!(
            error
                .to_string()
                .contains("synthetic asyncstd sendmail failure"),
            "{error:?}"
        );
    }
}
