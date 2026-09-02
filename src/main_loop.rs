use anyhow::Result;

use crate::application::Application;
use crate::connection::Connection;
use crate::msg::Message;

/// Run the main event loop.
///
/// Creates a Tokio runtime, bridges the synchronous crossbeam receiver
/// (from the stdio Connection) into an async tokio mpsc channel, then
/// hands control to `Application::run` which uses `tokio::select!` to
/// multiplex Emacs messages and agent events.
pub fn main_loop(connection: Connection) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async {
        let mut app = Application::new();
        let (emacs_tx, mut emacs_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

        // Bridge the crossbeam receiver into the tokio channel.
        // spawn_blocking runs on a dedicated thread that can block on
        // the synchronous crossbeam iterator without starving the async runtime.
        let crossbeam_rx = connection.receiver.clone();
        tokio::task::spawn_blocking(move || {
            for msg in crossbeam_rx {
                if emacs_tx.send(msg).is_err() {
                    break;
                }
            }
        });

        app.run(&connection.sender, &mut emacs_rx).await
    })
}
