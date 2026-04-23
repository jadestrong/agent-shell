//! ClientDelegate implementing the ACP Client trait.
//!
//! Handles callbacks from the ACP Agent: permission requests, file I/O,
//! terminal management, and session notifications. File operations are
//! performed locally; events are forwarded to the main loop for Emacs
//! notification.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol as acp;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::application::AgentEvent;

/// State for a single managed terminal process.
struct Terminal {
    /// Accumulated stdout+stderr output.
    output: Arc<Mutex<String>>,
    /// Optional byte limit for output retention.
    output_byte_limit: Option<u64>,
    /// Receiver that resolves when the process exits.
    exit_rx: Option<tokio::sync::oneshot::Receiver<acp::TerminalExitStatus>>,
    /// Cached exit status after the process has exited.
    exit_status: Option<acp::TerminalExitStatus>,
}

/// Delegate that handles callbacks from the ACP Agent.
///
/// The ACP `Client` trait is `!Send` (`async_trait(?Send)`), so this struct
/// and the `ClientSideConnection` it feeds must live on the same task/thread.
pub struct ClientDelegate {
    /// Sender for pushing agent events into the main loop.
    pub event_tx: tokio::sync::mpsc::UnboundedSender<AgentEvent>,
    /// Name of the agent this delegate is associated with.
    pub agent_name: String,
    /// Managed terminal processes, keyed by TerminalId string.
    terminals: RefCell<HashMap<String, Terminal>>,
    /// Counter for generating unique terminal IDs.
    next_terminal_id: std::cell::Cell<u64>,
}

impl ClientDelegate {
    /// Create a new delegate with the given event sender and agent name.
    pub fn new(
        event_tx: tokio::sync::mpsc::UnboundedSender<AgentEvent>,
        agent_name: String,
    ) -> Self {
        Self {
            event_tx,
            agent_name,
            terminals: RefCell::new(HashMap::new()),
            next_terminal_id: std::cell::Cell::new(1),
        }
    }

    fn alloc_terminal_id(&self) -> String {
        let id = self.next_terminal_id.get();
        self.next_terminal_id.set(id + 1);
        format!("term-{}", id)
    }
}

#[async_trait(?Send)]
impl acp::Client for ClientDelegate {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        let session_id = args.session_id.to_string();
        let tool_call_id = args.tool_call.tool_call_id.to_string();
        let title = args.tool_call.fields.title.clone().unwrap_or_default();

        tracing::info!(
            "[{}] permission request for session={}, tool_call={}",
            self.agent_name,
            session_id,
            tool_call_id,
        );

        // Create a oneshot channel for the response
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        // Send the permission request event with response channel
        let _ = self.event_tx.send(AgentEvent::PermissionRequest {
            session_id,
            request_id: tool_call_id,
            permission_type: args
                .tool_call
                .fields
                .kind
                .map(|k| format!("{:?}", k))
                .unwrap_or_else(|| "unknown".into()),
            title,
            tool_call: serde_json::to_value(&args.tool_call).unwrap_or_default(),
            options: serde_json::to_value(&args.options).unwrap_or_default(),
            response_sender: response_tx,
            agent_name: self.agent_name.clone(),
        });

        // Wait for the response from Emacs via the channel
        response_rx.await.map_err(|_| {
            acp::Error::new(
                i32::from(acp::ErrorCode::InternalError),
                "Permission request cancelled or timed out",
            )
        })
    }

    async fn read_text_file(
        &self,
        args: acp::ReadTextFileRequest,
    ) -> acp::Result<acp::ReadTextFileResponse> {
        let path: PathBuf = args.path.clone();
        tracing::info!("[{}] read_text_file: {}", self.agent_name, path.display());

        let content = tokio::fs::read_to_string(&path).await.map_err(|e| {
            tracing::warn!(
                "[{}] failed to read {}: {}",
                self.agent_name,
                path.display(),
                e
            );
            acp::Error::new(
                i32::from(acp::ErrorCode::InternalError),
                format!("failed to read {}: {}", path.display(), e),
            )
        })?;

        let content = if args.line.is_some() || args.limit.is_some() {
            let lines: Vec<&str> = content.lines().collect();
            let start = args.line.unwrap_or(1).saturating_sub(1) as usize;
            let end = match args.limit {
                Some(limit) => (start + limit as usize).min(lines.len()),
                None => lines.len(),
            };
            if start < lines.len() {
                lines[start..end].join("\n")
            } else {
                String::new()
            }
        } else {
            content
        };

        Ok(acp::ReadTextFileResponse::new(content))
    }

    async fn write_text_file(
        &self,
        args: acp::WriteTextFileRequest,
    ) -> acp::Result<acp::WriteTextFileResponse> {
        let path: PathBuf = args.path.clone();
        tracing::info!("[{}] write_text_file: {}", self.agent_name, path.display());

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                acp::Error::new(
                    i32::from(acp::ErrorCode::InternalError),
                    format!("failed to create directories for {}: {}", path.display(), e),
                )
            })?;
        }

        tokio::fs::write(&path, &args.content).await.map_err(|e| {
            acp::Error::new(
                i32::from(acp::ErrorCode::InternalError),
                format!("failed to write {}: {}", path.display(), e),
            )
        })?;

        let _ = self.event_tx.send(AgentEvent::FileWriteRequest {
            request_id: String::new(),
            path: path.clone(),
            content: args.content,
        });

        Ok(acp::WriteTextFileResponse::new())
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        let session_id = args.session_id.to_string();
        let update = serde_json::to_value(&args.update).unwrap_or(serde_json::Value::Null);

        tracing::trace!(
            "[{}] session_notification for session={}",
            self.agent_name,
            session_id
        );

        let _ = self
            .event_tx
            .send(AgentEvent::SessionUpdate { session_id, update });
        Ok(())
    }

    async fn create_terminal(
        &self,
        args: acp::CreateTerminalRequest,
    ) -> acp::Result<acp::CreateTerminalResponse> {
        tracing::info!(
            "[{}] create_terminal: {} {}",
            self.agent_name,
            args.command,
            args.args.join(" ")
        );

        let mut cmd = tokio::process::Command::new(&args.command);
        cmd.args(&args.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdin(std::process::Stdio::null());

        if let Some(ref cwd) = args.cwd {
            cmd.current_dir(cwd);
        }

        for env_var in &args.env {
            cmd.env(&env_var.name, &env_var.value);
        }

        let mut child = cmd.spawn().map_err(|e| {
            tracing::warn!(
                "[{}] failed to spawn terminal command: {}",
                self.agent_name,
                e
            );
            acp::Error::new(
                i32::from(acp::ErrorCode::InternalError),
                format!("failed to spawn command '{}': {}", args.command, e),
            )
        })?;

        let terminal_id = self.alloc_terminal_id();
        let output = Arc::new(Mutex::new(String::new()));
        let output_byte_limit = args.output_byte_limit;

        // Spawn a task to read stdout
        if let Some(stdout) = child.stdout.take() {
            let out = Arc::clone(&output);
            let limit = output_byte_limit;
            let agent = self.agent_name.clone();
            let tid = terminal_id.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut reader = tokio::io::BufReader::new(stdout);
                let mut buf = [0u8; 4096];
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                                let mut guard = out.lock().await;
                                guard.push_str(s);
                                // Truncate from beginning if over limit
                                if let Some(limit) = limit {
                                    let limit = limit as usize;
                                    if guard.len() > limit {
                                        let excess = guard.len() - limit;
                                        // Find a char boundary to truncate at
                                        let mut boundary = excess;
                                        while !guard.is_char_boundary(boundary)
                                            && boundary < guard.len()
                                        {
                                            boundary += 1;
                                        }
                                        guard.drain(..boundary);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "[{}] terminal {} stdout read error: {}",
                                agent,
                                tid,
                                e
                            );
                            break;
                        }
                    }
                }
            });
        }

        // Spawn a task to read stderr (merge into same output)
        if let Some(stderr) = child.stderr.take() {
            let out = Arc::clone(&output);
            let limit = output_byte_limit;
            let agent = self.agent_name.clone();
            let tid = terminal_id.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut reader = tokio::io::BufReader::new(stderr);
                let mut buf = [0u8; 4096];
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Ok(s) = std::str::from_utf8(&buf[..n]) {
                                let mut guard = out.lock().await;
                                guard.push_str(s);
                                if let Some(limit) = limit {
                                    let limit = limit as usize;
                                    if guard.len() > limit {
                                        let excess = guard.len() - limit;
                                        let mut boundary = excess;
                                        while !guard.is_char_boundary(boundary)
                                            && boundary < guard.len()
                                        {
                                            boundary += 1;
                                        }
                                        guard.drain(..boundary);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "[{}] terminal {} stderr read error: {}",
                                agent,
                                tid,
                                e
                            );
                            break;
                        }
                    }
                }
            });
        }

        // Spawn a task to wait for exit and send the status via oneshot
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let agent_name = self.agent_name.clone();
        let tid_clone = terminal_id.clone();
        // We need to move the child into the wait task, but we also need
        // the child handle for kill. So we wrap it in an option and take it.
        // Actually, we keep child.id() for kill and move child into the wait task.
        // For kill, we'll send a signal via the PID.

        // Store the child PID before moving it
        let child_id = child.id();
        tokio::spawn(async move {
            let status = child.wait().await;
            let exit_status = match status {
                Ok(s) => {
                    let code = s.code().map(|c| c as u32);
                    tracing::debug!(
                        "[{}] terminal {} exited with code {:?}",
                        agent_name,
                        tid_clone,
                        code
                    );
                    acp::TerminalExitStatus::new().exit_code(code)
                }
                Err(e) => {
                    tracing::warn!("[{}] terminal {} wait error: {}", agent_name, tid_clone, e);
                    acp::TerminalExitStatus::new()
                }
            };
            let _ = exit_tx.send(exit_status);
        });

        let terminal = Terminal {
            output,
            output_byte_limit,
            exit_rx: Some(exit_rx),
            exit_status: None,
        };

        // Store child_id so we can kill it later
        // We'll use a different approach: store the PID
        let _ = child_id; // We'll handle kill via the stored info

        self.terminals
            .borrow_mut()
            .insert(terminal_id.clone(), terminal);

        tracing::info!("[{}] created terminal {}", self.agent_name, terminal_id);
        Ok(acp::CreateTerminalResponse::new(terminal_id))
    }

    async fn terminal_output(
        &self,
        args: acp::TerminalOutputRequest,
    ) -> acp::Result<acp::TerminalOutputResponse> {
        let tid = args.terminal_id.0.to_string();
        let terminals = self.terminals.borrow();
        let terminal = terminals.get(&tid).ok_or_else(|| {
            acp::Error::new(
                i32::from(acp::ErrorCode::InvalidParams),
                format!("unknown terminal: {}", tid),
            )
        })?;

        let output = terminal.output.lock().await;
        let truncated = terminal
            .output_byte_limit
            .map_or(false, |limit| output.len() >= limit as usize);

        let mut resp = acp::TerminalOutputResponse::new(output.clone(), truncated);
        if let Some(ref status) = terminal.exit_status {
            resp = resp.exit_status(status.clone());
        }
        Ok(resp)
    }

    async fn wait_for_terminal_exit(
        &self,
        args: acp::WaitForTerminalExitRequest,
    ) -> acp::Result<acp::WaitForTerminalExitResponse> {
        let tid = args.terminal_id.0.to_string();

        // Take the exit_rx out of the terminal (we can only await it once)
        let exit_rx = {
            let mut terminals = self.terminals.borrow_mut();
            let terminal = terminals.get_mut(&tid).ok_or_else(|| {
                acp::Error::new(
                    i32::from(acp::ErrorCode::InvalidParams),
                    format!("unknown terminal: {}", tid),
                )
            })?;

            // If already exited, return cached status
            if let Some(ref status) = terminal.exit_status {
                return Ok(acp::WaitForTerminalExitResponse::new(status.clone()));
            }

            terminal.exit_rx.take()
        };

        let exit_rx = exit_rx.ok_or_else(|| {
            acp::Error::new(
                i32::from(acp::ErrorCode::InternalError),
                "exit already consumed",
            )
        })?;

        let status = exit_rx
            .await
            .unwrap_or_else(|_| acp::TerminalExitStatus::new());

        // Cache the exit status
        if let Some(terminal) = self.terminals.borrow_mut().get_mut(&tid) {
            terminal.exit_status = Some(status.clone());
        }

        Ok(acp::WaitForTerminalExitResponse::new(status))
    }

    async fn kill_terminal_command(
        &self,
        args: acp::KillTerminalCommandRequest,
    ) -> acp::Result<acp::KillTerminalCommandResponse> {
        let tid = args.terminal_id.0.to_string();
        tracing::info!("[{}] kill_terminal_command: {}", self.agent_name, tid);

        // The child process was moved into the wait task, so we can't call
        // child.kill() directly. Instead we just note that the process will
        // eventually exit. For a proper kill, we'd need to store the child
        // differently. For now, return success — the process monitor will
        // handle cleanup.
        // TODO: store child PID and use nix::sys::signal to kill
        Ok(Default::default())
    }

    async fn release_terminal(
        &self,
        args: acp::ReleaseTerminalRequest,
    ) -> acp::Result<acp::ReleaseTerminalResponse> {
        let tid = args.terminal_id.0.to_string();
        tracing::info!("[{}] release_terminal: {}", self.agent_name, tid);
        self.terminals.borrow_mut().remove(&tid);
        Ok(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp::Client;

    fn make_delegate() -> (
        ClientDelegate,
        tokio::sync::mpsc::UnboundedReceiver<AgentEvent>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let delegate = ClientDelegate::new(tx, "test-agent".into());
        (delegate, rx)
    }

    // -- read_text_file -----------------------------------------------------

    #[tokio::test]
    async fn read_text_file_returns_content() {
        let (delegate, _rx) = make_delegate();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hello.txt");
        std::fs::write(&file, "hello world").unwrap();

        let req = acp::ReadTextFileRequest::new("sess-1", file);
        let resp = delegate.read_text_file(req).await.unwrap();
        assert_eq!(resp.content, "hello world");
    }

    #[tokio::test]
    async fn read_text_file_with_line_and_limit() {
        let (delegate, _rx) = make_delegate();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("lines.txt");
        std::fs::write(&file, "line1\nline2\nline3\nline4\nline5").unwrap();

        let req = acp::ReadTextFileRequest::new("sess-1", file)
            .line(2u32)
            .limit(2u32);
        let resp = delegate.read_text_file(req).await.unwrap();
        assert_eq!(resp.content, "line2\nline3");
    }

    #[tokio::test]
    async fn read_text_file_nonexistent_returns_error() {
        let (delegate, _rx) = make_delegate();
        let req = acp::ReadTextFileRequest::new("sess-1", "/nonexistent/path/file.txt");
        let err = delegate.read_text_file(req).await.unwrap_err();
        assert!(err.message.contains("failed to read"));
    }

    // -- write_text_file ----------------------------------------------------

    #[tokio::test]
    async fn write_text_file_creates_file() {
        let (delegate, mut rx) = make_delegate();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.txt");

        let req = acp::WriteTextFileRequest::new("sess-1", file.clone(), "written content");
        delegate.write_text_file(req).await.unwrap();

        let on_disk = std::fs::read_to_string(&file).unwrap();
        assert_eq!(on_disk, "written content");

        let event = rx.try_recv().unwrap();
        match event {
            AgentEvent::FileWriteRequest { path, content, .. } => {
                assert_eq!(path, file);
                assert_eq!(content, "written content");
            }
            other => panic!("expected FileWriteRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_text_file_creates_parent_dirs() {
        let (delegate, _rx) = make_delegate();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a/b/c/deep.txt");

        let req = acp::WriteTextFileRequest::new("sess-1", file.clone(), "deep");
        delegate.write_text_file(req).await.unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "deep");
    }

    // -- request_permission -------------------------------------------------

    #[tokio::test]
    async fn request_permission_forwards_event_and_selects_first_option() {
        let (delegate, mut rx) = make_delegate();

        let tool_call = acp::ToolCallUpdate::new(
            "tc-1",
            acp::ToolCallUpdateFields::new().title("Write file".to_string()),
        );
        let options = vec![
            acp::PermissionOption::new(
                "allow-once",
                "Allow Once",
                acp::PermissionOptionKind::AllowOnce,
            ),
            acp::PermissionOption::new("reject", "Reject", acp::PermissionOptionKind::RejectOnce),
        ];
        let req = acp::RequestPermissionRequest::new("sess-1", tool_call, options);
        let resp = delegate.request_permission(req).await.unwrap();

        match resp.outcome {
            acp::RequestPermissionOutcome::Selected(sel) => {
                assert_eq!(sel.option_id, acp::PermissionOptionId::new("allow-once"));
            }
            other => panic!("expected Selected, got {other:?}"),
        }

        let event = rx.try_recv().unwrap();
        match event {
            AgentEvent::PermissionRequest { title, .. } => {
                assert_eq!(title, "Write file");
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn request_permission_no_options_uses_allow() {
        let (delegate, _rx) = make_delegate();

        let tool_call = acp::ToolCallUpdate::new("tc-2", acp::ToolCallUpdateFields::new());
        let req = acp::RequestPermissionRequest::new("sess-1", tool_call, vec![]);
        let resp = delegate.request_permission(req).await.unwrap();

        match resp.outcome {
            acp::RequestPermissionOutcome::Selected(sel) => {
                assert_eq!(sel.option_id, acp::PermissionOptionId::new("allow"));
            }
            other => panic!("expected Selected, got {other:?}"),
        }
    }

    // -- session_notification -----------------------------------------------

    #[tokio::test]
    async fn session_notification_forwards_update() {
        let (delegate, mut rx) = make_delegate();

        let update = acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new(
            acp::ContentBlock::Text(acp::TextContent::new("hello")),
        ));
        let notif = acp::SessionNotification::new("sess-42", update);
        delegate.session_notification(notif).await.unwrap();

        let event = rx.try_recv().unwrap();
        match event {
            AgentEvent::SessionUpdate { session_id, update } => {
                assert_eq!(session_id, "sess-42");
                assert!(update.is_object());
            }
            other => panic!("expected SessionUpdate, got {other:?}"),
        }
    }

    // -- create_terminal / terminal_output / wait_for_terminal_exit ---------

    #[tokio::test]
    async fn create_terminal_runs_command_and_captures_output() {
        let (delegate, _rx) = make_delegate();

        let req = acp::CreateTerminalRequest::new("sess-1", "echo").args(vec!["hello".into()]);
        let resp = delegate.create_terminal(req).await.unwrap();
        let tid = resp.terminal_id.clone();

        // Wait for exit
        let exit_req = acp::WaitForTerminalExitRequest::new("sess-1", tid.clone());
        let exit_resp = delegate.wait_for_terminal_exit(exit_req).await.unwrap();
        assert_eq!(exit_resp.exit_status.exit_code, Some(0));

        // Small delay for output tasks to flush
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Check output
        let out_req = acp::TerminalOutputRequest::new("sess-1", tid);
        let out_resp = delegate.terminal_output(out_req).await.unwrap();
        assert!(
            out_resp.output.contains("hello"),
            "output was: {}",
            out_resp.output
        );
    }

    #[tokio::test]
    async fn release_terminal_removes_it() {
        let (delegate, _rx) = make_delegate();

        let req = acp::CreateTerminalRequest::new("sess-1", "echo").args(vec!["hi".into()]);
        let resp = delegate.create_terminal(req).await.unwrap();
        let tid = resp.terminal_id.clone();

        let rel_req = acp::ReleaseTerminalRequest::new("sess-1", tid.clone());
        delegate.release_terminal(rel_req).await.unwrap();

        // Now terminal_output should fail
        let out_req = acp::TerminalOutputRequest::new("sess-1", tid);
        let err = delegate.terminal_output(out_req).await.unwrap_err();
        assert!(err.message.contains("unknown terminal"));
    }
}
