use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Result;
use crossbeam_channel::Sender;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::agent::AgentConnection;
use crate::config::Config;
use agent_client_protocol as acp;
use agent_client_protocol::{ConnectionTo, Responder, on_receive_notification, on_receive_request};
use agent_client_protocol::role::acp::Agent;
use agent_client_protocol_tokio::AcpAgent;

use crate::msg::{
    Message, Notification, Request, Response, AUTH_REQUIRED, INTERNAL_ERROR, INVALID_PARAMS,
    METHOD_NOT_FOUND,
};

// ---------------------------------------------------------------------------
// Emacs -> Proxy request methods
// ---------------------------------------------------------------------------

pub mod methods {
    pub const CONNECT_AGENT: &str = "acp/connectAgent";
    pub const NEW_SESSION: &str = "acp/newSession";
    pub const PROMPT: &str = "acp/prompt";
    pub const CANCEL: &str = "acp/cancel";
    pub const LIST_AGENTS: &str = "acp/listAgents";
    pub const LIST_SESSIONS: &str = "acp/listSessions";
    pub const AUTHENTICATE: &str = "acp/authenticate";
    pub const SET_MODEL: &str = "acp/setModel";
    pub const SET_MODE: &str = "acp/setMode";
    pub const RESPOND_PERMISSION: &str = "acp/respondPermission";
}

// ---------------------------------------------------------------------------
// Proxy -> Emacs notification methods
// ---------------------------------------------------------------------------

pub mod notifications {
    pub const SESSION_UPDATE: &str = "acp/sessionUpdate";
    pub const PERMISSION_REQUEST: &str = "acp/permissionRequest";
    pub const AGENT_DISCONNECTED: &str = "acp/agentDisconnected";
    pub const AUTH_REQUIRED: &str = "acp/authRequired";
    pub const FILE_CHANGED: &str = "acp/fileChanged";
    pub const AGENT_EXT_NOTIFICATION: &str = "acp/agentExtNotification";
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum SessionStatus {
    Active,
    Idle,
    Error(String),
}

#[derive(Debug, Clone, Default)]
pub struct SessionMessageState {
    pub last_message_type: Option<String>,
    pub last_stream_message_id: Option<String>,
}

#[derive(Debug)]
pub struct SessionState {
    pub id: String,
    pub agent_name: String,
    pub status: SessionStatus,
    pub message_state: SessionMessageState,
}

// ---------------------------------------------------------------------------
// Agent events (from ACP Agent processes to the main loop)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AgentEvent {
    SessionUpdate {
        session_id: String,
        update: serde_json::Value,
    },
    PermissionRequest {
        session_id: String,
        request_id: String,
        permission_type: String,
        title: String,
        tool_call: serde_json::Value,
        options: serde_json::Value,
        responder: Responder<acp::schema::RequestPermissionResponse>,
        agent_name: String,
    },
    FileReadRequest {
        request_id: String,
        path: PathBuf,
    },
    FileWriteRequest {
        request_id: String,
        path: PathBuf,
        content: String,
    },
    AgentExited {
        agent_name: String,
        exit_code: Option<i32>,
    },
    ExtNotification {
        method: String,
        params: serde_json::Value,
    },
}

// ---------------------------------------------------------------------------
// Pending request tracking
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PendingRequest {
    pub emacs_request_id: crate::msg::RequestId,
    pub method: String,
    pub created_at: std::time::Instant,
}

/// Tracks a permission request awaiting Emacs user response
pub struct PendingPermissionRequest {
    pub request_id: String,
    pub session_id: String,
    pub agent_name: String,
    pub responder: Responder<acp::schema::RequestPermissionResponse>,
    pub created_at: std::time::Instant,
}

#[derive(Debug)]
enum BackupWriterEvent {
    Append { path: PathBuf, content: String },
}

#[derive(Debug, Default, Clone)]
struct ToolCallSnapshot {
    title: Option<String>,
    kind: Option<String>,
    description: Option<String>,
    command: Option<String>,
    raw_input: Option<serde_json::Value>,
}

#[derive(Debug, Default, Clone)]
struct SessionTranscriptState {
    last_entry_type: Option<String>,
    tool_calls: HashMap<String, ToolCallSnapshot>,
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// Core application state managing agents, sessions, and request routing.
#[allow(dead_code)]
pub struct Application {
    config: Config,
    /// Connected ACP agents, keyed by agent name.
    agents: HashMap<String, AgentConnection>,
    /// Active sessions, keyed by session ID.
    sessions: HashMap<String, SessionState>,
    /// Transcript file path keyed by session ID.
    session_transcript_paths: HashMap<String, PathBuf>,
    /// Transcript render state keyed by session ID.
    session_transcript_state: HashMap<String, SessionTranscriptState>,
    /// Sender for transcript events; writer runs in background task.
    backup_writer_tx: tokio::sync::mpsc::UnboundedSender<BackupWriterEvent>,
    /// Monotonically increasing ID for outgoing requests to agents.
    next_request_id: AtomicI64,
    /// Requests sent to agents that are awaiting a response.
    pending_requests: HashMap<crate::msg::RequestId, PendingRequest>,
    /// Permission requests awaiting Emacs user responses.
    pending_permissions: HashMap<String, PendingPermissionRequest>,
    /// Sender for agent events — handed to agent tasks so they can push events.
    agent_event_tx: tokio::sync::mpsc::UnboundedSender<AgentEvent>,
    /// Receiver for agent events — taken once in run().
    agent_event_rx: Option<tokio::sync::mpsc::UnboundedReceiver<AgentEvent>>,
}

impl Application {
    fn make_message_id(next_request_id: &AtomicI64) -> String {
        let seq = next_request_id.fetch_add(1, Ordering::Relaxed);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        format!("msg-{}-{}", ts, seq)
    }

    fn assign_message_id_for_update(&mut self, session_id: &str, update: &mut serde_json::Value) {
        let next_request_id = &self.next_request_id;
        let Some(session) = self.sessions.get_mut(session_id) else {
            return;
        };
        let Some(update_type) = update.get("sessionUpdate").and_then(|v| v.as_str()) else {
            return;
        };
        let stream_type = match update_type {
            "agent_message_chunk" => {
                let content_type = update
                    .get("content")
                    .and_then(|v| v.get("type"))
                    .and_then(|v| v.as_str());
                if content_type == Some("text") {
                    Some("agent_message_chunk:text")
                } else {
                    None
                }
            }
            "agent_thought_chunk" => Some("agent_thought_chunk"),
            _ => None,
        };

        let message_id = match update_type {
            "tool_call" | "tool_call_update" => {
                // Tool lifecycle updates must break stream reuse so later
                // thought/text chunks start a new message block.
                session.message_state.last_message_type = None;
                session.message_state.last_stream_message_id = None;
                update
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                    .unwrap_or_else(|| Self::make_message_id(next_request_id))
            }
            "user_message_chunk" => {
                session.message_state.last_message_type = None;
                session.message_state.last_stream_message_id = None;
                Self::make_message_id(next_request_id)
            }
            "plan" => {
                session.message_state.last_message_type = None;
                session.message_state.last_stream_message_id = None;
                "plan".to_string()
            }
            _ => {
                if let Some(stream_type) = stream_type {
                    if session.message_state.last_message_type.as_deref() == Some(stream_type) {
                        session
                            .message_state
                            .last_stream_message_id
                            .clone()
                            .unwrap_or_else(|| {
                                let id = Self::make_message_id(next_request_id);
                                session.message_state.last_stream_message_id = Some(id.clone());
                                id
                            })
                    } else {
                        let id = Self::make_message_id(next_request_id);
                        session.message_state.last_message_type = Some(stream_type.to_string());
                        session.message_state.last_stream_message_id = Some(id.clone());
                        id
                    }
                } else {
                    session.message_state.last_message_type = None;
                    session.message_state.last_stream_message_id = None;
                    Self::make_message_id(next_request_id)
                }
            }
        };

        if let Some(obj) = update.as_object_mut() {
            obj.insert(
                "messageId".to_string(),
                serde_json::Value::String(message_id),
            );
        }
    }

    fn reset_session_stream_state(&mut self, session_id: &str) {
        if let Some(session) = self.sessions.get_mut(session_id) {
            session.message_state.last_message_type = None;
            session.message_state.last_stream_message_id = None;
        }
    }

    fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
        let z = days_since_epoch + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = mp + if mp < 10 { 3 } else { -9 };
        let year = y + if m <= 2 { 1 } else { 0 };
        (year as i32, m as u32, d as u32)
    }

    fn now_label() -> String {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let hour = sod / 3_600;
        let minute = (sod % 3_600) / 60;
        let second = sod % 60;
        let (year, month, day) = Self::civil_from_days(days);
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        )
    }

    fn now_file_label() -> String {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let hour = sod / 3_600;
        let minute = (sod % 3_600) / 60;
        let second = sod % 60;
        let (year, month, day) = Self::civil_from_days(days);
        format!("{:04}-{:02}-{:02}-{:02}-{:02}-{:02}", year, month, day, hour, minute, second)
    }

    fn session_transcript_file_path(cwd: &str, file_label: &str) -> PathBuf {
        PathBuf::from(cwd)
            .join(".agent-shell")
            .join("transcripts")
            .join(format!("{file_label}.md"))
    }

    fn ensure_gitignore(project_root: &str) {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(project_root)
            .arg("rev-parse")
            .arg("--show-toplevel")
            .output();
        let Ok(output) = output else { return };
        if !output.status.success() {
            return;
        }
        let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if root.is_empty() {
            return;
        }
        let ignore_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("check-ignore")
            .arg("-q")
            .arg(".agent-shell")
            .status();
        if ignore_status.map(|s| s.success()).unwrap_or(false) {
            return;
        }
        let gitignore = PathBuf::from(&root).join(".gitignore");
        let entry = "/.agent-shell/\n";
        let already_has = std::fs::read_to_string(&gitignore)
            .ok()
            .map(|content| content.contains("/.agent-shell/"))
            .unwrap_or(false);
        if !already_has {
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&gitignore)
                .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()));
        }
    }

    fn init_session_transcript(&mut self, session_id: &str, agent_name: &str, cwd: &str) {
        let file_label = Self::now_file_label();
        let path = Self::session_transcript_file_path(cwd, &file_label);
        Self::ensure_gitignore(cwd);
        let ts = Self::now_label();
        let header = format!(
            "# Agent Shell Transcript\n\n**Agent:** {}\n**Started:** {}\n**Working Directory:** {}\n\n---\n\n",
            agent_name, ts, cwd
        );
        self.session_transcript_paths
            .insert(session_id.to_string(), path.clone());
        self.session_transcript_state
            .insert(session_id.to_string(), SessionTranscriptState::default());
        self.enqueue_backup_append(path, header);
    }

    fn enqueue_backup_append(&self, path: PathBuf, content: String) {
        if let Err(e) = self
            .backup_writer_tx
            .send(BackupWriterEvent::Append { path, content })
        {
            tracing::warn!("failed to enqueue backup write: {}", e);
        }
    }

    fn append_session_transcript(&self, session_id: &str, markdown: &str) {
        let Some(path) = self.session_transcript_paths.get(session_id) else {
            return;
        };
        self.enqueue_backup_append(path.clone(), markdown.to_string());
    }

    fn extract_text_from_value(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Array(items) => {
                let mut parts = Vec::new();
                for item in items {
                    if let Some(t) = item.get("type").and_then(|v| v.as_str()) {
                        if t == "text" {
                            if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    parts.push(text.to_string());
                                }
                            }
                        } else if let Some(content_text) = item
                            .get("content")
                            .and_then(|v| v.get("text"))
                            .and_then(|v| v.as_str())
                        {
                            if !content_text.is_empty() {
                                parts.push(content_text.to_string());
                            }
                        }
                    } else if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                        if !text.is_empty() {
                            parts.push(text.to_string());
                        }
                    }
                }
                parts.join("\n\n")
            }
            _ => String::new(),
        }
    }

    fn indent_markdown_headers(text: &str) -> String {
        let mut in_code_block: Option<usize> = None;
        let mut out = Vec::new();
        for line in text.split('\n') {
            let backtick_run = line.chars().take_while(|c| *c == '`').count();
            if backtick_run >= 3 {
                if let Some(current) = in_code_block {
                    if backtick_run >= current {
                        in_code_block = None;
                    }
                } else {
                    in_code_block = Some(backtick_run);
                }
                out.push(line.to_string());
                continue;
            }
            if in_code_block.is_none() {
                let hash_run = line.chars().take_while(|c| *c == '#').count();
                if hash_run > 0 && line.chars().nth(hash_run) == Some(' ') {
                    let new_level = std::cmp::min(6, hash_run + 2);
                    let new_hashes = "#".repeat(new_level);
                    let replaced = format!("{} {}", new_hashes, &line[(hash_run + 1)..]);
                    out.push(replaced);
                    continue;
                }
            }
            out.push(line.to_string());
        }
        out.join("\n")
    }

    fn longest_backtick_run(text: &str) -> usize {
        let mut max_run = 0;
        let mut current = 0;
        for ch in text.chars() {
            if ch == '`' {
                current += 1;
                if current > max_run {
                    max_run = current;
                }
            } else {
                current = 0;
            }
        }
        max_run
    }

    fn extract_tool_parameters(raw_input: Option<&serde_json::Value>) -> Option<String> {
        let Some(raw_input) = raw_input else { return None };
        let Some(obj) = raw_input.as_object() else { return None };
        let excluded = ["command", "description", "plan"];
        let mut lines = Vec::new();
        for (k, v) in obj {
            if excluded.contains(&k.as_str()) {
                continue;
            }
            if v.is_null() {
                continue;
            }
            if let Some(s) = v.as_str() {
                if s.trim().is_empty() {
                    continue;
                }
                lines.push(format!("{}: {}", k, s));
                continue;
            }
            if let Some(n) = v.as_i64() {
                lines.push(format!("{}: {}", k, n));
                continue;
            }
            if let Some(n) = v.as_u64() {
                lines.push(format!("{}: {}", k, n));
                continue;
            }
            if let Some(n) = v.as_f64() {
                lines.push(format!("{}: {}", k, n));
                continue;
            }
            if let Some(b) = v.as_bool() {
                lines.push(format!("{}: {}", k, if b { "true" } else { "false" }));
                continue;
            }
            if let Ok(serialized) = serde_json::to_string(v) {
                lines.push(format!("{}: {}", k, serialized));
            }
        }
        if lines.is_empty() { None } else { Some(lines.join("\n")) }
    }

    fn make_tool_call_entry(
        status: Option<&str>,
        title: Option<&str>,
        kind: Option<&str>,
        description: Option<&str>,
        command: Option<&str>,
        parameters: Option<&str>,
        output: &str,
    ) -> String {
        let trimmed = output.trim();
        let fence_len = std::cmp::max(3, Self::longest_backtick_run(trimmed) + 1);
        let fence = "`".repeat(fence_len);
        let mut entry = String::new();
        entry.push_str(&format!(
            "\n\n### Tool Call [{}]: {}\n",
            status.unwrap_or("no status"),
            title.unwrap_or("")
        ));
        if let Some(kind) = kind {
            if !kind.is_empty() {
                entry.push_str(&format!("\n**Tool:** {}", kind));
            }
        }
        entry.push_str(&format!("\n**Timestamp:** {}", Self::now_label()));
        if let Some(description) = description {
            if !description.is_empty() {
                entry.push_str(&format!("\n**Description:** {}", description));
            }
        }
        if let Some(command) = command {
            if !command.is_empty() {
                entry.push_str(&format!("\n**Command:** {}", command));
            }
        }
        if let Some(parameters) = parameters {
            if !parameters.is_empty() {
                entry.push_str(&format!("\n**Parameters:**\n{}", parameters));
            }
        }
        entry.push_str("\n\n");
        entry.push_str(&fence);
        entry.push('\n');
        entry.push_str(trimmed);
        entry.push('\n');
        entry.push_str(&fence);
        entry.push('\n');
        entry
    }

    fn start_transcript_section_if_needed(
        &mut self,
        session_id: &str,
        entry_type: &str,
        heading: &str,
    ) {
        let needs_new_heading = self
            .session_transcript_state
            .get(session_id)
            .and_then(|s| s.last_entry_type.as_deref())
            != Some(entry_type);
        if needs_new_heading {
            let md = format!("\n## {} ({})\n\n", heading, Self::now_label());
            self.append_session_transcript(session_id, &md);
            let state = self
                .session_transcript_state
                .entry(session_id.to_string())
                .or_default();
            state.last_entry_type = Some(entry_type.to_string());
        }
    }

    fn transcript_prompt_message(&mut self, session_id: &str, params: &serde_json::Value) {
        let message = params.get("message").cloned().unwrap_or(serde_json::Value::Null);
        let text = Self::extract_text_from_value(&message);
        self.start_transcript_section_if_needed(session_id, "user_prompt", "User");
        let body = if text.is_empty() {
            "_(empty prompt)_\n\n".to_string()
        } else {
            format!(
                "{}\n\n",
                Self::indent_markdown_headers(text.trim())
            )
        };
        self.append_session_transcript(session_id, &body);
    }

    fn transcript_session_update(&mut self, session_id: &str, update: &serde_json::Value) {
        let update_type = update
            .get("sessionUpdate")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match update_type {
            "agent_message_chunk" => {
                let text = update
                    .get("content")
                    .and_then(|v| v.get("text"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    return;
                }
                self.start_transcript_section_if_needed(session_id, "agent_message_chunk", "Agent");
                let body = Self::indent_markdown_headers(text);
                self.append_session_transcript(session_id, &body);
            }
            "agent_thought_chunk" => {
                let text = update
                    .get("content")
                    .and_then(|v| v.get("text"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    return;
                }
                self.start_transcript_section_if_needed(session_id, "agent_thought_chunk", "Agent's Thoughts");
                let body = Self::indent_markdown_headers(text);
                self.append_session_transcript(session_id, &body);
            }
            "user_message_chunk" => {
                let text = update
                    .get("content")
                    .and_then(|v| v.get("text"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    return;
                }
                self.start_transcript_section_if_needed(session_id, "user_message_chunk", "User");
                let body = format!(
                    "> {}\n",
                    Self::indent_markdown_headers(text)
                );
                self.append_session_transcript(session_id, &body);
            }
            "tool_call" | "tool_call_update" => {
                let tool_call_id = update
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let status = update.get("status").and_then(|v| v.as_str());
                let title = update.get("title").and_then(|v| v.as_str());
                let kind = update.get("kind").and_then(|v| v.as_str());
                let raw_input = update.get("rawInput");
                let description = raw_input.and_then(|v| v.get("description")).and_then(|v| v.as_str());
                let command = raw_input.and_then(|v| v.get("command")).and_then(|v| v.as_str());

                let state = self
                    .session_transcript_state
                    .entry(session_id.to_string())
                    .or_default();
                state.last_entry_type = Some("tool_call".to_string());
                if !tool_call_id.is_empty() {
                    let snapshot = state.tool_calls.entry(tool_call_id.to_string()).or_default();
                    if let Some(title) = title {
                        if !title.is_empty() {
                            snapshot.title = Some(title.to_string());
                        }
                    }
                    if let Some(kind) = kind {
                        if !kind.is_empty() {
                            snapshot.kind = Some(kind.to_string());
                        }
                    }
                    if let Some(description) = description {
                        if !description.is_empty() {
                            snapshot.description = Some(description.to_string());
                        }
                    }
                    if let Some(command) = command {
                        if !command.is_empty() {
                            snapshot.command = Some(command.to_string());
                        }
                    }
                    if let Some(raw_input) = raw_input {
                        snapshot.raw_input = Some(raw_input.clone());
                    }
                }

                if matches!(status, Some("completed") | Some("failed")) {
                    let output = update
                        .get("content")
                        .and_then(|v| v.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.get("text").and_then(|v| v.as_str()))
                                .collect::<Vec<_>>()
                                .join("\n\n")
                        })
                        .unwrap_or_default();
                    let output = format!("\n\n{}\n\n", output);

                    let (snap_title, snap_kind, snap_desc, snap_cmd, snap_raw) = state
                        .tool_calls
                        .get(tool_call_id)
                        .map(|s| {
                            (
                                s.title.as_deref(),
                                s.kind.as_deref(),
                                s.description.as_deref(),
                                s.command.as_deref(),
                                s.raw_input.as_ref(),
                            )
                        })
                        .unwrap_or((None, None, None, None, None));
                    let parameters = Self::extract_tool_parameters(snap_raw);
                    let entry = Self::make_tool_call_entry(
                        status,
                        snap_title,
                        snap_kind,
                        snap_desc,
                        snap_cmd,
                        parameters.as_deref(),
                        &output,
                    );
                    self.append_session_transcript(session_id, &entry);
                }
            }
            _ => {}
        }
    }

    fn spawn_backup_writer() -> tokio::sync::mpsc::UnboundedSender<BackupWriterEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<BackupWriterEvent>();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            while let Some(event) = rx.recv().await {
                match event {
                    BackupWriterEvent::Append { path, content } => {
                        if let Some(parent) = path.parent() {
                            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                tracing::warn!(
                                    "failed to create backup directory {}: {}",
                                    parent.display(),
                                    e
                                );
                                continue;
                            }
                        }
                        let mut file = match tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&path)
                            .await
                        {
                            Ok(file) => file,
                            Err(e) => {
                                tracing::warn!(
                                    "failed to open backup file {}: {}",
                                    path.display(),
                                    e
                                );
                                continue;
                            }
                        };
                        if let Err(e) = file.write_all(content.as_bytes()).await {
                            tracing::warn!(
                                "failed to append backup file {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
        });
        tx
    }

    pub fn new(config: Config) -> Self {
        let (agent_event_tx, agent_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let backup_writer_tx = Self::spawn_backup_writer();
        Self {
            config,
            agents: HashMap::new(),
            sessions: HashMap::new(),
            session_transcript_paths: HashMap::new(),
            session_transcript_state: HashMap::new(),
            backup_writer_tx,
            next_request_id: AtomicI64::new(1),
            pending_requests: HashMap::new(),
            pending_permissions: HashMap::new(),
            agent_event_tx,
            agent_event_rx: Some(agent_event_rx),
        }
    }

    /// Returns a sender that agent tasks can use to push events into the main loop.
    pub fn agent_event_sender(&self) -> tokio::sync::mpsc::UnboundedSender<AgentEvent> {
        self.agent_event_tx.clone()
    }

    /// Allocate the next request ID for outgoing requests to agents.
    #[allow(dead_code)]
    pub fn next_request_id(&self) -> i64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    // -----------------------------------------------------------------------
    // Main run loop
    // -----------------------------------------------------------------------

    /// Main run loop — select on Emacs messages and agent events.
    pub async fn run(
        &mut self,
        emacs_sender: &Sender<Message>,
        emacs_receiver: &mut UnboundedReceiver<Message>,
    ) -> Result<()> {
        let mut agent_rx = self
            .agent_event_rx
            .take()
            .expect("run() must only be called once");

        loop {
            tokio::select! {
                msg = emacs_receiver.recv() => {
                    match msg {
                        Some(message) => {
                            self.handle_emacs_message(message, emacs_sender).await?;
                        }
                        None => {
                            tracing::info!("Emacs channel closed, shutting down");
                            break;
                        }
                    }
                }
                event = agent_rx.recv() => {
                    match event {
                        Some(agent_event) => {
                            self.handle_agent_event(agent_event, emacs_sender).await?;
                        }
                        None => {
                            tracing::info!("Agent event channel closed");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Emacs message dispatch
    // -----------------------------------------------------------------------

    async fn handle_emacs_message(
        &mut self,
        msg: Message,
        emacs_sender: &Sender<Message>,
    ) -> Result<()> {
        match msg {
            Message::Request(req) => {
                tracing::info!(
                    "Received request: {} (id={} params={})",
                    req.method,
                    req.id,
                    req.params
                );
                self.handle_request(req, emacs_sender).await?;
            }
            Message::Notification(notif) => {
                tracing::info!("Received notification: {} params={}", notif.method, notif.params);
                self.handle_notification(notif)?;
            }
            Message::Response(resp) => {
                tracing::info!("Received response for id={}", resp.id);
                // Responses from Emacs to our requests — not expected yet
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Request routing
    // -----------------------------------------------------------------------

    async fn handle_request(&mut self, req: Request, emacs_sender: &Sender<Message>) -> Result<()> {
        // For prompt requests, spawn a local task so the main loop stays free
        // to forward session update notifications while the prompt is in flight.
        if req.method == methods::PROMPT {
            if let Some(session_id) = req.params.get("sessionId").and_then(|v| v.as_str()) {
                self.reset_session_stream_state(session_id);
                self.transcript_prompt_message(session_id, &req.params);
            }
            self.spawn_prompt(req.id.clone(), req.params.clone(), emacs_sender.clone());
            return Ok(());
        }

        let response = match req.method.as_str() {
            methods::CONNECT_AGENT => {
                self.handle_connect_agent(req.id.clone(), req.params.clone())
                    .await
            }
            methods::NEW_SESSION => {
                self.handle_new_session(req.id.clone(), req.params.clone(), emacs_sender)
                    .await
            }
            methods::CANCEL => self.handle_cancel(req.id.clone(), req.params.clone()).await,
            methods::LIST_AGENTS => self.handle_list_agents(req.id.clone()),
            methods::LIST_SESSIONS => self.handle_list_sessions(req.id.clone()),
            methods::AUTHENTICATE => {
                self.handle_authenticate(req.id.clone(), req.params.clone())
                    .await
            }
            methods::SET_MODEL => {
                self.handle_set_model(req.id.clone(), req.params.clone())
                    .await
            }
            methods::SET_MODE => {
                self.handle_set_mode(req.id.clone(), req.params.clone())
                    .await
            }
            methods::RESPOND_PERMISSION => {
                self.handle_respond_permission(req.id.clone(), req.params.clone())
            }
            _ => {
                tracing::warn!("Unknown method: {}", req.method);
                Response::new_err(
                    req.id,
                    METHOD_NOT_FOUND,
                    format!("Method not found: {}", req.method),
                )
            }
        };
        emacs_sender.send(Message::Response(response))?;
        Ok(())
    }

    fn handle_notification(&mut self, _notif: Notification) -> Result<()> {
        // Notifications don't require a response — just log for now
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Stub handlers — will be implemented in tasks 6.x
    // -----------------------------------------------------------------------

    async fn handle_connect_agent(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse agentName from params
        let agent_name = match params.get("agentName").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing agentName".into());
            }
        };

        // Return cached agent if already connected.
        if let Some(existing) = self.agents.get(&agent_name) {
            return Response::new_ok(
                id,
                serde_json::json!({
                    "capabilities": existing.capabilities,
                    "authMethods": existing.auth_methods,
                }),
            );
        }

        // Look up agent config or build from request params.
        let agent_config = match self.config.agents.get(&agent_name) {
            Some(config) => config.clone(),
            None => {
                let command = match params.get("command").and_then(|v| v.as_str()) {
                    Some(cmd) => cmd.to_string(),
                    None => {
                        return Response::new_err(
                            id,
                            INVALID_PARAMS,
                            format!("unknown agent: {} (missing command)", agent_name),
                        );
                    }
                };
                let args = params
                    .get("args")
                    .and_then(|v| v.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let env = params.get("env").and_then(|v| v.as_object()).map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect::<std::collections::HashMap<String, String>>()
                });
                let config = crate::config::AgentConfig {
                    command,
                    args,
                    env,
                    default_mode: None,
                    default_model: None,
                };
                self.config.agents.insert(agent_name.clone(), config.clone());
                config
            }
        };

        // Build AcpAgent: prepend NAME=value pairs so from_args parses them as env vars
        let env_args = agent_config.env.as_ref().into_iter()
            .flat_map(|env| env.iter().map(|(k, v)| format!("{}={}", k, v)));
        let cmd_args = std::iter::once(agent_config.command.clone())
            .chain(agent_config.args.iter().cloned());
        let all_args: Vec<String> = env_args.chain(cmd_args).collect();
        let acp_agent = match AcpAgent::from_args(all_args) {
            Ok(a) => a,
            Err(e) => return Response::new_err(id, INTERNAL_ERROR, format!("spawn failed: {}", e)),
        };

        // Add stderr debug logging
        let agent_name_log = agent_name.clone();
        let acp_agent = acp_agent.with_debug(move |line, dir| {
            use agent_client_protocol_tokio::LineDirection;
            match dir {
                LineDirection::Stderr => tracing::debug!("[{} stderr] {}", agent_name_log, line),
                LineDirection::Stdin => tracing::trace!("[{} stdin] {}", agent_name_log, line),
                LineDirection::Stdout => tracing::trace!("[{} stdout] {}", agent_name_log, line),
            }
        });

        let event_tx = self.agent_event_tx.clone();
        let agent_name_task = agent_name.clone();

        // Oneshot to receive the connection handle and init result from the spawned task
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<
            Result<
                (ConnectionTo<Agent>, serde_json::Value, serde_json::Value),
                String,
            >,
        >();

        tokio::task::spawn_local({
            let event_tx2 = event_tx.clone();
            let agent_name2 = agent_name_task.clone();
            async move {
                let version = env!("CARGO_PKG_VERSION");
                let result = acp::role::acp::Client
                    .builder()
                    .on_receive_notification(
                        {
                            let event_tx = event_tx.clone();
                            async move |notif: acp::schema::AgentNotification, _cx| {
                                match notif {
                                    acp::schema::AgentNotification::SessionNotification(sn) => {
                                        let session_id = sn.session_id.to_string();
                                        let update = serde_json::to_value(&sn.update)
                                            .unwrap_or_default();
                                        let _ = event_tx.send(AgentEvent::SessionUpdate {
                                            session_id,
                                            update,
                                        });
                                    }
                                    acp::schema::AgentNotification::ExtNotification(en) => {
                                        let method = en.method.to_string();
                                        let params = serde_json::from_str(en.params.get())
                                            .unwrap_or_default();
                                        let _ = event_tx.send(AgentEvent::ExtNotification {
                                            method,
                                            params,
                                        });
                                    }
                                    _ => {}
                                }
                                Ok(())
                            }
                        },
                        on_receive_notification!(),
                    )
                    .on_receive_request(
                        {
                            let event_tx = event_tx.clone();
                            let agent_name = agent_name_task.clone();
                            async move |req: acp::schema::RequestPermissionRequest,
                                        responder: Responder<acp::schema::RequestPermissionResponse>,
                                        cx| {
                                let session_id = req.session_id.to_string();
                                let request_id = req.tool_call.tool_call_id.to_string();
                                let title = req.tool_call.fields.title.clone().unwrap_or_default();
                                let tool_call = serde_json::to_value(&req.tool_call)
                                    .unwrap_or_default();
                                let options = serde_json::to_value(&req.options)
                                    .unwrap_or_default();
                                let _ = event_tx.send(AgentEvent::PermissionRequest {
                                    session_id,
                                    request_id,
                                    permission_type: req
                                        .tool_call
                                        .fields
                                        .kind
                                        .map(|k| format!("{:?}", k))
                                        .unwrap_or_else(|| "unknown".into()),
                                    title,
                                    tool_call,
                                    options,
                                    responder,
                                    agent_name: agent_name.clone(),
                                });
                                Ok(())
                            }
                        },
                        on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |req: acp::schema::ReadTextFileRequest,
                                    responder: Responder<acp::schema::ReadTextFileResponse>,
                                    cx| {
                            let path = req.path.clone();
                            cx.spawn(async move {
                                match tokio::fs::read_to_string(&path).await {
                                    Ok(mut content) => {
                                        // Apply line/limit slicing
                                        if req.line.is_some() || req.limit.is_some() {
                                            let lines: Vec<&str> = content.lines().collect();
                                            let start = req.line
                                                .unwrap_or(1)
                                                .saturating_sub(1) as usize;
                                            let end = match req.limit {
                                                Some(l) => (start + l as usize).min(lines.len()),
                                                None => lines.len(),
                                            };
                                            content = if start < lines.len() {
                                                lines[start..end].join("\n")
                                            } else {
                                                String::new()
                                            };
                                        }
                                        responder.respond(
                                            acp::schema::ReadTextFileResponse::new(content)
                                        )?;
                                    }
                                    Err(e) => {
                                        responder.respond_with_internal_error(
                                            format!("failed to read {}: {}", path.display(), e)
                                        )?;
                                    }
                                }
                                Ok(())
                            })?;
                            Ok(())
                        },
                        on_receive_request!(),
                    )
                    .on_receive_request(
                        {
                            let event_tx = event_tx.clone();
                            async move |req: acp::schema::WriteTextFileRequest,
                                        responder: Responder<acp::schema::WriteTextFileResponse>,
                                        cx| {
                                let path = req.path.clone();
                                let content = req.content.clone();
                                let event_tx2 = event_tx.clone();
                                cx.spawn(async move {
                                    if let Some(parent) = path.parent() {
                                        let _ = tokio::fs::create_dir_all(parent).await;
                                    }
                                    match tokio::fs::write(&path, &content).await {
                                        Ok(()) => {
                                            let _ = event_tx2.send(AgentEvent::FileWriteRequest {
                                                request_id: String::new(),
                                                path: path.clone(),
                                                content,
                                            });
                                            responder.respond(
                                                acp::schema::WriteTextFileResponse::new()
                                            )?;
                                        }
                                        Err(e) => {
                                            responder.respond_with_internal_error(
                                                format!("failed to write {}: {}", path.display(), e)
                                            )?;
                                        }
                                    }
                                    Ok(())
                                })?;
                                Ok(())
                            }
                        },
                        on_receive_request!(),
                    )
                    .connect_with(acp_agent, async move |cx: ConnectionTo<Agent>| {
                        tracing::info!("Spawned agent '{}'", agent_name_task);
                        // ACP initialize handshake
                        let init = cx.send_request(
                            acp::schema::InitializeRequest::new(acp::schema::ProtocolVersion::V1)
                                .client_capabilities(
                                    acp::schema::ClientCapabilities::default()
                                        .fs(acp::schema::FileSystemCapabilities::default()
                                            .read_text_file(true)
                                            .write_text_file(true))
                                        .terminal(true),
                                )
                                .client_info(acp::schema::Implementation::new(
                                    "emacs-acp-proxy",
                                    version,
                                )),
                        )
                        .block_task()
                        .await;

                        match init {
                            Ok(resp) => {
                                let capabilities =
                                    serde_json::to_value(&resp.agent_capabilities)
                                        .unwrap_or_default();
                                let auth_methods =
                                    serde_json::to_value(&resp.auth_methods).unwrap_or_default();
                                tracing::info!(
                                    "Agent '{}' initialized: auth_methods={}",
                                    agent_name_task,
                                    resp.auth_methods.len()
                                );
                                ready_tx.send(Ok((cx.clone(), capabilities, auth_methods))).ok();
                            }
                            Err(e) => {
                                ready_tx.send(Err(format!("initialize failed: {}", e))).ok();
                                return Err(e);
                            }
                        }

                        // Keep the connection alive until the agent disconnects
                        std::future::pending::<()>().await;
                        Ok(())
                    })
                    .await;

                // Agent disconnected — extract exit code from error message if possible
                let exit_code = match &result {
                    Err(e) => {
                        let msg = e.to_string();
                        // "Process exited with exit status: N"
                        msg.rsplit(':')
                            .next()
                            .and_then(|s| s.trim().parse::<i32>().ok())
                    }
                    Ok(()) => Some(0),
                };
                tracing::info!("Agent '{}' exited with code: {:?}", agent_name2, exit_code);
                let _ = event_tx2.send(AgentEvent::AgentExited {
                    agent_name: agent_name2,
                    exit_code,
                });
            }
        });

        // Wait for initialization result
        match ready_rx.await {
            Ok(Ok((cx, capabilities, auth_methods))) => {
                self.agents.insert(
                    agent_name,
                    AgentConnection { connection: cx, capabilities: capabilities.clone(), auth_methods: auth_methods.clone() },
                );
                Response::new_ok(
                    id,
                    serde_json::json!({
                        "capabilities": capabilities,
                        "authMethods": auth_methods,
                    }),
                )
            }
            Ok(Err(msg)) => Response::new_err(id, INTERNAL_ERROR, msg),
            Err(_) => Response::new_err(id, INTERNAL_ERROR, "agent task dropped".into()),
        }
    }

    async fn handle_new_session(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
        emacs_sender: &Sender<Message>,
    ) -> Response {
        // Parse agentName from params (required)
        let agent_name = match params.get("agentName").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing agentName".into());
            }
        };

        // Parse cwd from params (optional, defaults to ".")
        let cwd = params.get("cwd").and_then(|v| v.as_str()).unwrap_or(".");

        // Find the connected agent
        let agent = match self.agents.get(&agent_name) {
            Some(a) => a,
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                );
            }
        };

        let connection = agent.connection.clone();
        let request = acp::schema::NewSessionRequest::new(cwd);
        tracing::debug!(
            "Sending new_session request to agent '{}' with cwd='{}'",
            agent_name,
            cwd
        );
        match connection.send_request(request).block_task().await {
            Ok(resp) => {
                let session_id_str = resp.session_id.to_string();

                // Store session state
                self.sessions.insert(
                    session_id_str.clone(),
                    SessionState {
                        id: session_id_str.clone(),
                        agent_name: agent_name.clone(),
                        status: SessionStatus::Active,
                        message_state: SessionMessageState::default(),
                    },
                );
                self.init_session_transcript(&session_id_str, &agent_name, cwd);

                // Build response with session info
                let mut result = serde_json::json!({
                    "sessionId": session_id_str,
                });

                if let Some(modes) = &resp.modes {
                    result["modes"] = serde_json::to_value(modes).unwrap_or_default();
                }

                if let Some(models) = &resp.models {
                    result["models"] = serde_json::to_value(models).unwrap_or_default();
                }

                if let Some(config_options) = &resp.config_options {
                    result["configOptions"] =
                        serde_json::to_value(config_options).unwrap_or_default();
                }

                Response::new_ok(id, result)
            }
            Err(e) => {
                // Check for AuthRequired error
                if e.code == acp::schema::ErrorCode::AuthRequired.into() {
                    // Send acp/authRequired notification to Emacs
                    let notif = Notification {
                        method: notifications::AUTH_REQUIRED.into(),
                        params: serde_json::json!({
                            "agentName": agent_name,
                            "message": e.message,
                        }),
                    };
                    let _ = emacs_sender.send(Message::Notification(notif));
                    return Response::new_err(id, AUTH_REQUIRED, e.message);
                }
                Response::new_err(id, INTERNAL_ERROR, format!("new_session failed: {}", e))
            }
        }
    }

    /// Spawn the prompt request as a background local task.
    ///
    /// This allows the main `select!` loop to keep processing agent events
    /// (session update notifications) while the prompt is in flight, enabling
    /// streaming output to Emacs.
    fn spawn_prompt(
        &self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
        emacs_sender: Sender<Message>,
    ) {
        // --- synchronous validation ---
        let session_id = match params.get("sessionId").and_then(|v| v.as_str()) {
            Some(sid) => sid.to_string(),
            None => {
                let _ = emacs_sender.send(Message::Response(Response::new_err(
                    id,
                    INVALID_PARAMS,
                    "missing sessionId".into(),
                )));
                return;
            }
        };

        let message = match params.get("message") {
            Some(msg) => msg.clone(),
            None => {
                let _ = emacs_sender.send(Message::Response(Response::new_err(
                    id,
                    INVALID_PARAMS,
                    "missing message".into(),
                )));
                return;
            }
        };

        let agent_name = match self.sessions.get(&session_id) {
            Some(session) => session.agent_name.clone(),
            None => {
                let _ = emacs_sender.send(Message::Response(Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("session not found: {}", session_id),
                )));
                return;
            }
        };

        let connection = match self.agents.get(&agent_name) {
            Some(a) => a.connection.clone(),
            None => {
                let _ = emacs_sender.send(Message::Response(Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                )));
                return;
            }
        };

        let content_blocks: Vec<acp::schema::ContentBlock> = if let Some(text) = message.as_str() {
            vec![acp::schema::ContentBlock::Text(acp::schema::TextContent::new(text))]
        } else {
            match serde_json::from_value(message) {
                Ok(blocks) => blocks,
                Err(e) => {
                    let _ = emacs_sender.send(Message::Response(Response::new_err(
                        id,
                        INVALID_PARAMS,
                        format!("invalid message format: {}", e),
                    )));
                    return;
                }
            }
        };

        let transcript_path = self.session_transcript_paths.get(&session_id).cloned();
        let backup_writer_tx = self.backup_writer_tx.clone();

        // --- spawn the actual ACP call on the local set ---
        tokio::task::spawn_local(async move {
            let acp_session_id = acp::schema::SessionId::new(session_id.clone());
            let request = acp::schema::PromptRequest::new(acp_session_id, content_blocks);

            let response = match connection.send_request(request).block_task().await {
                Ok(resp) => match serde_json::to_value(&resp) {
                    Ok(mut val) => {
                        if let Some(obj) = val.as_object_mut() {
                            obj.insert(
                                "sessionId".to_string(),
                                serde_json::Value::String(session_id.clone()),
                            );
                        }
                        let stop_reason = val.get("stopReason").and_then(|v| v.as_str());
                        if stop_reason == Some("end_turn") {
                            if let Some(path) = transcript_path.clone() {
                                let _ = backup_writer_tx.send(BackupWriterEvent::Append {
                                    path,
                                    content: "\n\n".to_string(),
                                });
                            }
                        }
                        Response::new_ok(id, val)
                    }
                    Err(e) => Response::new_err(
                        id,
                        INTERNAL_ERROR,
                        format!("failed to serialize PromptResponse: {}", e),
                    ),
                },
                Err(e) => {
                    tracing::warn!("prompt failed for session {}: {}", session_id, e);
                    Response::new_err(id, INTERNAL_ERROR, format!("prompt failed: {}", e))
                }
            };
            let _ = emacs_sender.send(Message::Response(response));
        });
    }

    async fn handle_cancel(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse sessionId from params
        let session_id = match params.get("sessionId").and_then(|v| v.as_str()) {
            Some(sid) => sid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing sessionId".into());
            }
        };

        // Find the session to get the agent name
        let agent_name = match self.sessions.get(&session_id) {
            Some(session) => session.agent_name.clone(),
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("session not found: {}", session_id),
                );
            }
        };

        // Find the agent
        let agent = match self.agents.get(&agent_name) {
            Some(a) => a,
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                );
            }
        };

        // Send cancel notification to the agent
        let notification = acp::schema::CancelNotification::new(session_id);
        match agent.connection.send_notification(notification) {
            Ok(()) => Response::new_ok(id, serde_json::json!({})),
            Err(e) => Response::new_err(id, INTERNAL_ERROR, format!("cancel failed: {}", e)),
        }
    }

    fn handle_list_sessions(&self, id: crate::msg::RequestId) -> Response {
        let sessions: Vec<_> = self
            .sessions
            .values()
            .map(|s| {
                serde_json::json!({
                    "sessionId": s.id,
                    "agentName": s.agent_name,
                    "status": format!("{:?}", s.status),
                })
            })
            .collect();
        Response::new_ok(id, serde_json::json!({ "sessions": sessions }))
    }

    fn handle_list_agents(&self, id: crate::msg::RequestId) -> Response {
        let mut agents: Vec<_> = self.config.agents.keys().cloned().collect();
        agents.sort();
        Response::new_ok(id, serde_json::json!({ "agents": agents }))
    }

    async fn handle_authenticate(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse agentName from params (required)
        let agent_name = match params.get("agentName").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing agentName".into());
            }
        };

        // Parse authMethodId from params (required)
        let auth_method_id = match params.get("authMethodId").and_then(|v| v.as_str()) {
            Some(mid) => mid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing authMethodId".into());
            }
        };

        // Find the connected agent
        let agent = match self.agents.get(&agent_name) {
            Some(a) => a,
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                );
            }
        };

        // Forward authenticate request to the agent
        let connection = agent.connection.clone();
        let request = acp::schema::AuthenticateRequest::new(
            acp::schema::AuthMethodId::new(auth_method_id),
        );
        match connection.send_request(request).block_task().await {
            Ok(_response) => Response::new_ok(id, serde_json::json!({})),
            Err(e) => {
                tracing::warn!("authenticate failed for agent {}: {}", agent_name, e);
                Response::new_err(id, INTERNAL_ERROR, format!("authenticate failed: {}", e))
            }
        }
    }

    async fn handle_set_model(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse sessionId from params (required)
        let session_id = match params.get("sessionId").and_then(|v| v.as_str()) {
            Some(sid) => sid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing sessionId".into());
            }
        };

        // Parse modelId from params (required)
        let model_id = match params.get("modelId").and_then(|v| v.as_str()) {
            Some(mid) => mid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing modelId".into());
            }
        };

        // Find the session to get the agent name
        let agent_name = match self.sessions.get(&session_id) {
            Some(session) => session.agent_name.clone(),
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("session not found: {}", session_id),
                );
            }
        };

        // Find the agent
        let agent = match self.agents.get(&agent_name) {
            Some(a) => a,
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                );
            }
        };

        // Forward set_session_model request to the agent
        let connection = agent.connection.clone();
        let acp_session_id = acp::schema::SessionId::new(session_id.clone());
        let acp_model_id = acp::schema::ModelId::new(model_id.clone());
        let request = acp::schema::SetSessionModelRequest::new(acp_session_id, acp_model_id);
        match connection.send_request(request).block_task().await {
            Ok(_response) => Response::new_ok(id, serde_json::json!({})),
            Err(e) => {
                tracing::warn!("set_session_model failed for session {}: {}", session_id, e);
                Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("set_session_model failed: {}", e),
                )
            }
        }
    }

    async fn handle_set_mode(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse sessionId from params (required)
        let session_id = match params.get("sessionId").and_then(|v| v.as_str()) {
            Some(sid) => sid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing sessionId".into());
            }
        };

        // Parse modeId from params (required)
        let mode_id = match params.get("modeId").and_then(|v| v.as_str()) {
            Some(mid) => mid.to_string(),
            None => {
                return Response::new_err(id, INVALID_PARAMS, "missing modeId".into());
            }
        };

        // Find the session to get the agent name
        let agent_name = match self.sessions.get(&session_id) {
            Some(session) => session.agent_name.clone(),
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("session not found: {}", session_id),
                );
            }
        };

        // Find the agent
        let agent = match self.agents.get(&agent_name) {
            Some(a) => a,
            None => {
                return Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("agent not connected: {}", agent_name),
                );
            }
        };

        // Forward set_session_mode request to the agent
        let connection = agent.connection.clone();
        let acp_session_id = acp::schema::SessionId::new(session_id.clone());
        let acp_mode_id = acp::schema::SessionModeId::new(mode_id.clone());
        let request = acp::schema::SetSessionModeRequest::new(acp_session_id, acp_mode_id);
        match connection.send_request(request).block_task().await {
            Ok(_response) => Response::new_ok(id, serde_json::json!({})),
            Err(e) => {
                tracing::warn!("set_session_mode failed for session {}: {}", session_id, e);
                Response::new_err(
                    id,
                    INTERNAL_ERROR,
                    format!("set_session_mode failed: {}", e),
                )
            }
        }
    }

    fn handle_respond_permission(
        &mut self,
        id: crate::msg::RequestId,
        params: serde_json::Value,
    ) -> Response {
        // Parse fields from params
        let request_id = params
            .get("requestId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let option_id = params
            .get("optionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        tracing::info!(
            "respondPermission: requestId={}, sessionId={}, optionId={}",
            request_id,
            session_id,
            option_id
        );

        // Find the pending permission request
        if let Some(pending) = self.pending_permissions.remove(&request_id) {
            // Create the ACP response
            let outcome = acp::schema::RequestPermissionOutcome::Selected(
                acp::schema::SelectedPermissionOutcome::new(option_id.clone()),
            );
            let response = acp::schema::RequestPermissionResponse::new(outcome);

            // Respond via the stored Responder
            if let Err(e) = pending.responder.respond(response) {
                tracing::warn!(
                    "Failed to send permission response for request {}: {}",
                    request_id,
                    e
                );
            }

            Response::new_ok(id, serde_json::json!({"success": true}))
        } else {
            tracing::warn!("Permission request {} not found", request_id);
            Response::new_err(
                id,
                crate::msg::INVALID_REQUEST,
                format!("Permission request {} not found", request_id),
            )
        }
    }

    // -----------------------------------------------------------------------
    // Agent event handling
    // -----------------------------------------------------------------------

    async fn handle_agent_event(
        &mut self,
        event: AgentEvent,
        emacs_sender: &Sender<Message>,
    ) -> Result<()> {
        let notification = match event {
            AgentEvent::SessionUpdate {
                session_id,
                mut update,
            } => {
                self.assign_message_id_for_update(&session_id, &mut update);
                self.transcript_session_update(&session_id, &update);
                Notification {
                    method: notifications::SESSION_UPDATE.into(),
                    params: serde_json::json!({
                        "sessionId": session_id,
                        "update": update,
                    }),
                }
            }
            AgentEvent::PermissionRequest {
                session_id,
                request_id,
                permission_type,
                title,
                tool_call,
                options,
                responder,
                agent_name,
            } => {
                // Store the pending permission request
                let pending = PendingPermissionRequest {
                    request_id: request_id.clone(),
                    session_id: session_id.clone(),
                    agent_name: agent_name.clone(),
                    responder,
                    created_at: std::time::Instant::now(),
                };
                self.pending_permissions.insert(request_id.clone(), pending);

                // Send notification to Emacs
                Notification {
                    method: notifications::PERMISSION_REQUEST.into(),
                    params: serde_json::json!({
                        "sessionId": session_id,
                        "requestId": request_id,
                        "permissionType": permission_type,
                        "title": title,
                        "toolCall": tool_call,
                        "options": options,
                    }),
                }
            }
            AgentEvent::FileReadRequest { request_id, path } => {
                // File read requests are handled internally by the proxy,
                // not forwarded to Emacs. For now, log and return.
                tracing::debug!("File read request {} for {}", request_id, path.display());
                return Ok(());
            }
            AgentEvent::FileWriteRequest {
                request_id,
                path,
                content: _,
            } => {
                // File write requests are handled internally, then we notify
                // Emacs that a file changed.
                tracing::debug!("File write request {} for {}", request_id, path.display());
                Notification {
                    method: notifications::FILE_CHANGED.into(),
                    params: serde_json::json!({
                        "path": path.to_string_lossy(),
                    }),
                }
            }
            AgentEvent::ExtNotification { method, params } => {
                Notification {
                    method: notifications::AGENT_EXT_NOTIFICATION.into(),
                    params: serde_json::json!({
                        "method": method,
                        "params": params,
                    }),
                }
            }
            AgentEvent::AgentExited {
                agent_name,
                exit_code,
            } => {
                // Clean up sessions associated with this agent
                self.sessions.retain(|_, s| s.agent_name != agent_name);
                self.session_transcript_paths
                    .retain(|session_id, _| self.sessions.contains_key(session_id));
                self.session_transcript_state
                    .retain(|session_id, _| self.sessions.contains_key(session_id));
                self.agents.remove(&agent_name);

                Notification {
                    method: notifications::AGENT_DISCONNECTED.into(),
                    params: serde_json::json!({
                        "agentName": agent_name,
                        "exitCode": exit_code,
                    }),
                }
            }
        };

        tracing::trace!("emacs_send_begin: {}", notification.method);
        emacs_sender.send(Message::Notification(notification))?;
        tracing::trace!("emacs_send_done");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::RequestId;

    fn make_app() -> Application {
        Application::new(Config::default())
    }

    fn make_sender() -> (Sender<Message>, crossbeam_channel::Receiver<Message>) {
        crossbeam_channel::unbounded()
    }

    // -- Method routing -----------------------------------------------------

    #[tokio::test]
    async fn known_method_routes_to_handler() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        // LIST_SESSIONS is now implemented and returns a success response
        let req = Request {
            id: RequestId::from(1i64),
            method: methods::LIST_SESSIONS.into(),
            params: serde_json::Value::Null,
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                // Should return ok with empty sessions list, not Method Not Found
                assert!(resp.error.is_none(), "expected ok response");
                let result = resp.result.unwrap();
                assert_eq!(result["sessions"], serde_json::json!([]));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_agents_returns_configured_agents() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(3i64),
            method: methods::LIST_AGENTS.into(),
            params: serde_json::Value::Null,
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                assert!(resp.error.is_none(), "expected ok response");
                let result = resp.result.unwrap();
                assert!(result["agents"].is_array());
                assert!(result["agents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|a| a == "claude"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(2i64),
            method: "unknown/method".into(),
            params: serde_json::Value::Null,
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, METHOD_NOT_FOUND);
                assert!(err.message.contains("Method not found"));
                assert!(err.message.contains("unknown/method"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn all_known_methods_are_routed() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let known_methods = [
            methods::CONNECT_AGENT,
            methods::NEW_SESSION,
            methods::PROMPT,
            methods::CANCEL,
            methods::LIST_AGENTS,
            methods::LIST_SESSIONS,
            methods::AUTHENTICATE,
            methods::SET_MODEL,
            methods::SET_MODE,
            methods::RESPOND_PERMISSION,
        ];

        for (i, method) in known_methods.iter().enumerate() {
            let req = Request {
                id: RequestId::from(i as i64),
                method: method.to_string(),
                params: serde_json::Value::Null,
            };
            app.handle_request(req, &tx).await.unwrap();

            let msg = rx.recv().unwrap();
            match msg {
                Message::Response(resp) => {
                    // Handlers may return success (e.g. LIST_SESSIONS) or an error
                    // (e.g. INVALID_PARAMS for missing required params).
                    // The key assertion: none should return Method Not Found.
                    if let Some(err) = resp.error {
                        assert_ne!(
                            err.code, METHOD_NOT_FOUND,
                            "method {} should route to a handler, not return Method Not Found",
                            method
                        );
                    }
                }
                other => panic!("expected Response for {method}, got {other:?}"),
            }
        }
    }

    // -- Agent event handling -----------------------------------------------

    #[tokio::test]
    async fn agent_session_update_sends_notification() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let event = AgentEvent::SessionUpdate {
            session_id: "sess-1".into(),
            update: serde_json::json!({"text": "hello"}),
        };
        app.handle_agent_event(event, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Notification(notif) => {
                assert_eq!(notif.method, notifications::SESSION_UPDATE);
                assert_eq!(notif.params["sessionId"], "sess-1");
                assert_eq!(notif.params["update"]["text"], "hello");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    /// Build a `Responder<RequestPermissionResponse>` backed by a no-op sink, for use in tests.
    async fn make_test_responder(
    ) -> acp::Responder<acp::schema::RequestPermissionResponse> {
        use agent_client_protocol::{ByteStreams, ConnectionTo, Handled, Responder as AcpResponder};
        use agent_client_protocol::role::UntypedRole;
        use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

        let (client_w, server_r) = tokio::io::duplex(4096);
        let (server_w, client_r) = tokio::io::duplex(4096);

        let server_transport = ByteStreams::new(server_w.compat_write(), server_r.compat());
        let client_transport = ByteStreams::new(client_w.compat_write(), client_r.compat());

        let (responder_tx, responder_rx) = tokio::sync::oneshot::channel::<
            acp::Responder<acp::schema::RequestPermissionResponse>,
        >();

        tokio::task::spawn_local(async move {
            let _ = UntypedRole.builder()
                .on_receive_request(
                    {
                        let mut tx = Some(responder_tx);
                        async move |_req: acp::schema::RequestPermissionRequest,
                                    responder: AcpResponder<acp::schema::RequestPermissionResponse>,
                                    _cx| {
                            if let Some(tx) = tx.take() {
                                let _ = tx.send(responder);
                            }
                            Ok(())
                        }
                    },
                    on_receive_request!(),
                )
                .connect_to(server_transport)
                .await;
        });

        // Client: send a dummy RequestPermissionRequest to trigger the server handler
        tokio::task::spawn_local(async move {
            let _ = UntypedRole.builder()
                .connect_with(client_transport, async |cx: ConnectionTo<UntypedRole>| {
                    let _ = cx.send_request(acp::schema::RequestPermissionRequest::new(
                        "sess-test",
                        acp::schema::ToolCallUpdate::new("tc-test", acp::schema::ToolCallUpdateFields::new()),
                        vec![],
                    ));
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Ok(())
                })
                .await;
        });

        responder_rx.await.expect("responder should arrive")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn agent_permission_request_sends_notification() {
        use tokio::task::LocalSet;
        let local = LocalSet::new();
        local.run_until(async {
            let mut app = make_app();
            let (tx, rx) = make_sender();

            let responder = make_test_responder().await;
            let event = AgentEvent::PermissionRequest {
                session_id: "sess-1".into(),
                request_id: "perm-1".into(),
                permission_type: "file_write".into(),
                title: "Write to foo.rs".into(),
                tool_call: serde_json::json!({}),
                options: serde_json::json!([]),
                responder,
                agent_name: "test-agent".into(),
            };
            app.handle_agent_event(event, &tx).await.unwrap();

            let msg = rx.recv().unwrap();
            match msg {
                Message::Notification(notif) => {
                    assert_eq!(notif.method, notifications::PERMISSION_REQUEST);
                    assert_eq!(notif.params["permissionType"], "file_write");
                    assert_eq!(notif.params["title"], "Write to foo.rs");
                }
                other => panic!("expected Notification, got {other:?}"),
            }
        }).await;
    }

    #[tokio::test]
    async fn agent_exited_cleans_up_sessions_and_notifies() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        // Add a session for the agent
        app.sessions.insert(
            "sess-1".into(),
            SessionState {
                id: "sess-1".into(),
                agent_name: "claude".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );

        let event = AgentEvent::AgentExited {
            agent_name: "claude".into(),
            exit_code: Some(1),
        };
        app.handle_agent_event(event, &tx).await.unwrap();

        // Session should be cleaned up
        assert!(app.sessions.is_empty());
        // Agent should be removed (was never inserted, but verify it's not there)
        assert!(!app.agents.contains_key("claude"));

        let msg = rx.recv().unwrap();
        match msg {
            Message::Notification(notif) => {
                assert_eq!(notif.method, notifications::AGENT_DISCONNECTED);
                assert_eq!(notif.params["agentName"], "claude");
                assert_eq!(notif.params["exitCode"], 1);
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_write_event_sends_file_changed_notification() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let event = AgentEvent::FileWriteRequest {
            request_id: "req-1".into(),
            path: PathBuf::from("/tmp/test.rs"),
            content: "fn main() {}".into(),
        };
        app.handle_agent_event(event, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Notification(notif) => {
                assert_eq!(notif.method, notifications::FILE_CHANGED);
                assert_eq!(notif.params["path"], "/tmp/test.rs");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_read_event_does_not_send_notification() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let event = AgentEvent::FileReadRequest {
            request_id: "req-1".into(),
            path: PathBuf::from("/tmp/test.rs"),
        };
        app.handle_agent_event(event, &tx).await.unwrap();

        // No notification should be sent for file reads
        assert!(rx.try_recv().is_err());
    }

    // -- connect_agent handler ----------------------------------------------

    #[tokio::test]
    async fn connect_agent_missing_agent_name_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(10i64),
            method: methods::CONNECT_AGENT.into(),
            params: serde_json::json!({}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing agentName"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connect_agent_unknown_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(11i64),
            method: methods::CONNECT_AGENT.into(),
            params: serde_json::json!({"agentName": "nonexistent"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("nonexistent"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_agent_spawn_failure_returns_error() {
        use crate::config::AgentConfig;
        use tokio::task::LocalSet;

        let local = LocalSet::new();
        local.run_until(async {
            let mut config = Config::default();
            config.agents.insert(
                "bad-agent".into(),
                AgentConfig {
                    command: "/nonexistent/binary/path".into(),
                    args: vec![],
                    env: None,
                    default_mode: None,
                    default_model: None,
                },
            );
            let mut app = Application::new(config);
            let (tx, rx) = make_sender();

            let req = Request {
                id: RequestId::from(12i64),
                method: methods::CONNECT_AGENT.into(),
                params: serde_json::json!({"agentName": "bad-agent"}),
            };
            app.handle_request(req, &tx).await.unwrap();

            let msg = rx.recv().unwrap();
            match msg {
                Message::Response(resp) => {
                    let err = resp.error.unwrap();
                    assert_eq!(err.code, INTERNAL_ERROR);
                    assert!(
                        err.message.contains("spawn failed")
                            || err.message.contains("agent task dropped")
                            || err.message.contains("No such file"),
                        "unexpected error: {}",
                        err.message
                    );
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }).await;
    }

    // -- new_session handler ------------------------------------------------

    #[tokio::test]
    async fn new_session_missing_agent_name_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(20i64),
            method: methods::NEW_SESSION.into(),
            params: serde_json::json!({}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing agentName"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn new_session_unknown_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(21i64),
            method: methods::NEW_SESSION.into(),
            params: serde_json::json!({"agentName": "nonexistent"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- list_sessions handler ----------------------------------------------

    #[tokio::test]
    async fn list_sessions_empty_returns_empty_list() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(30i64),
            method: methods::LIST_SESSIONS.into(),
            params: serde_json::Value::Null,
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                assert!(resp.error.is_none());
                let result = resp.result.unwrap();
                assert_eq!(result["sessions"], serde_json::json!([]));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_sessions_returns_stored_sessions() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        // Manually insert sessions
        app.sessions.insert(
            "sess-1".into(),
            SessionState {
                id: "sess-1".into(),
                agent_name: "claude".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );
        app.sessions.insert(
            "sess-2".into(),
            SessionState {
                id: "sess-2".into(),
                agent_name: "claude".into(),
                status: SessionStatus::Idle,
                message_state: SessionMessageState::default(),
            },
        );

        let req = Request {
            id: RequestId::from(31i64),
            method: methods::LIST_SESSIONS.into(),
            params: serde_json::Value::Null,
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                assert!(resp.error.is_none());
                let result = resp.result.unwrap();
                let sessions = result["sessions"].as_array().unwrap();
                assert_eq!(sessions.len(), 2);

                // Check that both session IDs are present
                let ids: Vec<&str> = sessions
                    .iter()
                    .map(|s| s["sessionId"].as_str().unwrap())
                    .collect();
                assert!(ids.contains(&"sess-1"));
                assert!(ids.contains(&"sess-2"));

                // Check agent names
                for s in sessions {
                    assert_eq!(s["agentName"], "claude");
                }
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- cancel handler -----------------------------------------------------

    #[tokio::test]
    async fn cancel_missing_session_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(40i64),
            method: methods::CANCEL.into(),
            params: serde_json::json!({}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing sessionId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_unknown_session_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(41i64),
            method: methods::CANCEL.into(),
            params: serde_json::json!({"sessionId": "nonexistent"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("session not found"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_session_with_missing_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        // Insert a session but don't insert the agent
        app.sessions.insert(
            "sess-orphan".into(),
            SessionState {
                id: "sess-orphan".into(),
                agent_name: "ghost-agent".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );

        let req = Request {
            id: RequestId::from(42i64),
            method: methods::CANCEL.into(),
            params: serde_json::json!({"sessionId": "sess-orphan"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- prompt handler -----------------------------------------------------

    #[tokio::test]
    async fn prompt_missing_session_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(50i64),
            method: methods::PROMPT.into(),
            params: serde_json::json!({"message": "hello"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing sessionId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn prompt_missing_message_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(51i64),
            method: methods::PROMPT.into(),
            params: serde_json::json!({"sessionId": "sess-1"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing message"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn prompt_unknown_session_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(52i64),
            method: methods::PROMPT.into(),
            params: serde_json::json!({"sessionId": "nonexistent", "message": "hello"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("session not found"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn prompt_session_with_missing_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        // Insert a session but don't insert the agent
        app.sessions.insert(
            "sess-orphan".into(),
            SessionState {
                id: "sess-orphan".into(),
                agent_name: "ghost-agent".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );

        let req = Request {
            id: RequestId::from(53i64),
            method: methods::PROMPT.into(),
            params: serde_json::json!({"sessionId": "sess-orphan", "message": "hello"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- authenticate handler -----------------------------------------------

    #[tokio::test]
    async fn authenticate_missing_agent_name_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(60i64),
            method: methods::AUTHENTICATE.into(),
            params: serde_json::json!({"authMethodId": "oauth"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing agentName"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn authenticate_missing_auth_method_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(61i64),
            method: methods::AUTHENTICATE.into(),
            params: serde_json::json!({"agentName": "claude"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing authMethodId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn authenticate_unknown_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(62i64),
            method: methods::AUTHENTICATE.into(),
            params: serde_json::json!({"agentName": "nonexistent", "authMethodId": "oauth"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- set_model handler --------------------------------------------------

    #[tokio::test]
    async fn set_model_missing_session_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(70i64),
            method: methods::SET_MODEL.into(),
            params: serde_json::json!({"modelId": "claude-sonnet"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing sessionId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_model_missing_model_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(71i64),
            method: methods::SET_MODEL.into(),
            params: serde_json::json!({"sessionId": "sess-1"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing modelId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_model_unknown_session_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(72i64),
            method: methods::SET_MODEL.into(),
            params: serde_json::json!({"sessionId": "nonexistent", "modelId": "claude-sonnet"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("session not found"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_model_session_with_missing_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        app.sessions.insert(
            "sess-orphan".into(),
            SessionState {
                id: "sess-orphan".into(),
                agent_name: "ghost-agent".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );

        let req = Request {
            id: RequestId::from(73i64),
            method: methods::SET_MODEL.into(),
            params: serde_json::json!({"sessionId": "sess-orphan", "modelId": "claude-sonnet"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- set_mode handler ---------------------------------------------------

    #[tokio::test]
    async fn set_mode_missing_session_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(80i64),
            method: methods::SET_MODE.into(),
            params: serde_json::json!({"modeId": "code"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing sessionId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_mode_missing_mode_id_returns_invalid_params() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(81i64),
            method: methods::SET_MODE.into(),
            params: serde_json::json!({"sessionId": "sess-1"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INVALID_PARAMS);
                assert!(err.message.contains("missing modeId"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_mode_unknown_session_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(82i64),
            method: methods::SET_MODE.into(),
            params: serde_json::json!({"sessionId": "nonexistent", "modeId": "code"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("session not found"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_mode_session_with_missing_agent_returns_error() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        app.sessions.insert(
            "sess-orphan".into(),
            SessionState {
                id: "sess-orphan".into(),
                agent_name: "ghost-agent".into(),
                status: SessionStatus::Active,
                message_state: SessionMessageState::default(),
            },
        );

        let req = Request {
            id: RequestId::from(83i64),
            method: methods::SET_MODE.into(),
            params: serde_json::json!({"sessionId": "sess-orphan", "modeId": "code"}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, INTERNAL_ERROR);
                assert!(err.message.contains("agent not connected"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    // -- respond_permission handler -----------------------------------------

    #[tokio::test(flavor = "current_thread")]
    async fn respond_permission_returns_success() {
        use tokio::task::LocalSet;
        let local = LocalSet::new();
        local.run_until(async {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let responder = make_test_responder().await;
        app.pending_permissions.insert(
            "perm-1".into(),
            PendingPermissionRequest {
                request_id: "perm-1".into(),
                session_id: "sess-1".into(),
                agent_name: "test-agent".into(),
                responder,
                created_at: std::time::Instant::now(),
            },
        );

        let req = Request {
            id: RequestId::from(90i64),
            method: methods::RESPOND_PERMISSION.into(),
            params: serde_json::json!({
                "requestId": "perm-1",
                "sessionId": "sess-1",
                "optionId": "allow_once"
            }),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                assert!(resp.error.is_none());
                assert!(resp.result.is_some());
            }
            other => panic!("expected Response, got {other:?}"),
        }
        }).await;
    }

    #[tokio::test]
    async fn respond_permission_with_empty_params_returns_success() {
        let mut app = make_app();
        let (tx, rx) = make_sender();

        let req = Request {
            id: RequestId::from(91i64),
            method: methods::RESPOND_PERMISSION.into(),
            params: serde_json::json!({}),
        };
        app.handle_request(req, &tx).await.unwrap();

        let msg = rx.recv().unwrap();
        match msg {
            Message::Response(resp) => {
                let err = resp.error.unwrap();
                assert_eq!(err.code, crate::msg::INVALID_REQUEST);
                assert!(err.message.contains("Permission request"));
                assert!(err.message.contains("not found"));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }
}
