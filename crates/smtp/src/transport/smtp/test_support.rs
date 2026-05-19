use std::{
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener},
    sync::mpsc,
    thread,
    time::Duration,
};

pub(super) struct LmtpServer {
    pub(super) address: SocketAddr,
    commands_rx: mpsc::Receiver<Vec<String>>,
    handle: thread::JoinHandle<()>,
}

impl LmtpServer {
    pub(super) fn commands(self) -> Vec<String> {
        let commands = self
            .commands_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        self.handle.join().unwrap();
        commands
    }
}

pub(super) fn spawn_lmtp_delivery_server() -> LmtpServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (commands_tx, commands_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(b"220 localhost\r\n").unwrap();

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut commands = Vec::new();

        let mut lhlo = String::new();
        reader.read_line(&mut lhlo).unwrap();
        commands.push(lhlo);
        stream
            .write_all(b"250-localhost\r\n250 8BITMIME\r\n")
            .unwrap();

        for response in [
            b"250 sender ok\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
            b"550 rcpt rejected\r\n".as_slice(),
            b"250 rcpt ok\r\n".as_slice(),
        ] {
            let mut command = String::new();
            reader.read_line(&mut command).unwrap();
            commands.push(command);
            stream.write_all(response).unwrap();
        }

        let mut data = String::new();
        reader.read_line(&mut data).unwrap();
        commands.push(data);
        stream.write_all(b"354 send message\r\n").unwrap();

        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line == ".\r\n" {
                break;
            }
        }

        stream
            .write_all(b"250 first recipient ok\r\n451 third recipient deferred\r\n")
            .unwrap();
        commands_tx.send(commands).unwrap();
    });

    LmtpServer {
        address,
        commands_rx,
        handle,
    }
}

pub(super) fn assert_lmtp_delivery_commands(commands: &[String]) {
    assert!(commands[0].starts_with("LHLO "));
    assert!(commands[1].starts_with("MAIL FROM:<sender@example.com>"));
    assert!(commands[2].starts_with("RCPT TO:<first@example.com>"));
    assert!(commands[3].starts_with("RCPT TO:<second@example.com>"));
    assert!(commands[4].starts_with("RCPT TO:<third@example.com>"));
    assert_eq!(commands[5], "DATA\r\n");
}
