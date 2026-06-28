use crate::msg::Message;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::{
    io::{self, stdin, stdout, Write},
    thread,
};

/// Bidirectional stdio connection between Emacs and the ACP Proxy.
///
/// Messages from Emacs arrive on `receiver`; messages to Emacs go through `sender`.
/// Outgoing messages use Content-Length framed JSON-RPC.
/// Incoming messages accept both Content-Length framing and NDJSON.
pub struct Connection {
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>,
}

impl Connection {
    /// Create a stdio connection, spawning reader and writer threads.
    ///
    /// The reader thread reads JSON-RPC messages from stdin and forwards them
    /// through a crossbeam channel. The writer thread receives messages from
    /// a crossbeam channel and writes them to stdout.
    pub fn stdio() -> (Connection, IoThreads) {
        let (writer_sender, writer_receiver) = unbounded::<Message>();
        let writer = thread::spawn(move || {
            let stdout = stdout();
            let mut stdout = stdout.lock();
            for msg in writer_receiver {
                let summary = match &msg {
                    Message::Notification(n) => format!("notif:{}", n.method),
                    Message::Response(r) => format!("resp:{}", r.id),
                    Message::Request(r) => format!("req:{}:{}", r.id, r.method),
                };
                tracing::trace!("stdout_write_begin: {}", summary);
                msg.write(&mut stdout)?;
                stdout.flush()?;
                tracing::trace!("stdout_write_done: {}", summary);
            }
            Ok(())
        });

        let (reader_sender, reader_receiver) = bounded::<Message>(0);
        let reader = thread::spawn(move || {
            let stdin = stdin();
            let mut stdin = stdin.lock();
            while let Some(msg) = Message::read(&mut stdin)? {
                if reader_sender.send(msg).is_err() {
                    // Main loop has shut down; stop reader thread quietly.
                    break;
                }
            }
            Ok(())
        });

        let threads = IoThreads { reader, writer };
        (
            Connection {
                sender: writer_sender,
                receiver: reader_receiver,
            },
            threads,
        )
    }
}

/// Handles for the reader and writer I/O threads.
///
/// Call `join()` during shutdown to wait for both threads to finish
/// and propagate any I/O errors.
pub struct IoThreads {
    reader: thread::JoinHandle<io::Result<()>>,
    writer: thread::JoinHandle<io::Result<()>>,
}

impl IoThreads {
    pub fn join(self) -> io::Result<()> {
        match self.reader.join() {
            Ok(r) => r?,
            Err(_) => return Err(io::Error::other("reader thread panicked")),
        }

        match self.writer.join() {
            Ok(r) => r,
            Err(_) => Err(io::Error::other("writer thread panicked")),
        }
    }
}
