//! ACP Agent process management and transport layer.
//!
//! `AgentClient` wraps a child process running an ACP-compatible agent server
//! (e.g. Claude Code CLI) and provides the ACP protocol connection via
//! `agent-client-protocol`'s `ClientSideConnection`.

use std::process::ExitStatus;

use agent_client_protocol as acp;
use agent_client_protocol::Agent;
use anyhow::{bail, Context, Result};
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::application::AgentEvent;
use crate::config::AgentConfig;
use crate::delegate::ClientDelegate;

/// Minimum ACP protocol version this client supports.
const MINIMUM_SUPPORTED_VERSION: acp::ProtocolVersion = acp::ProtocolVersion::V1;

/// Manages a single ACP Agent child process and its protocol connection.
pub struct AgentClient {
    /// Human-readable name for this agent (matches config key).
    pub name: String,
    /// ACP protocol connection (implements the `Agent` trait).
    pub connection: std::rc::Rc<acp::ClientSideConnection>,
    /// Handle for the background I/O task driving the ACP connection.
    _io_task: tokio::task::JoinHandle<()>,
    /// Handle for the stderr logging task.
    _stderr_task: tokio::task::JoinHandle<()>,
    /// Handle for the child process monitor task.
    _monitor_task: tokio::task::JoinHandle<()>,
    /// Agent capabilities received from the initialize response.
    pub capabilities: Option<acp::AgentCapabilities>,
    /// Authentication methods supported by the agent.
    pub auth_methods: Vec<acp::AuthMethod>,
}

impl AgentClient {
    /// Spawn an ACP Agent child process and establish the ACP protocol connection.
    ///
    /// This will:
    /// 1. Start the child process with piped stdin/stdout/stderr
    /// 2. Convert tokio I/O streams to futures-compatible streams via compat layer
    /// 3. Create a `ClientSideConnection` using the `agent-client-protocol` crate
    /// 4. Spawn background tasks for I/O, stderr logging, and process monitoring
    ///
    /// The caller must ensure this runs inside a `tokio::task::LocalSet` because
    /// the ACP `Client` trait is `!Send`.
    pub async fn spawn(
        name: String,
        config: &AgentConfig,
        event_tx: tokio::sync::mpsc::UnboundedSender<AgentEvent>,
    ) -> Result<Self> {
        // Build and spawn the child process
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        // Set environment variables if configured
        if let Some(ref env) = config.env {
            for (key, value) in env {
                cmd.env(key, value);
            }
        }

        let mut child = cmd.spawn().with_context(|| {
            format!(
                "failed to spawn agent '{}': {} {}",
                name,
                config.command,
                config.args.join(" ")
            )
        })?;

        tracing::info!(
            "Spawned agent '{}' (pid: {:?}): {} {}",
            name,
            child.id(),
            config.command,
            config.args.join(" ")
        );

        let stdin = child.stdin.take().context("failed to take agent stdin")?;
        let stdout = child.stdout.take().context("failed to take agent stdout")?;
        let stderr = child.stderr.take().context("failed to take agent stderr")?;

        // Convert tokio AsyncRead/AsyncWrite to futures AsyncRead/AsyncWrite
        // using tokio-util's compat layer.
        let stdin_compat = stdin.compat_write();
        let stdout_compat = stdout.compat();

        // Create the ACP client delegate (handles callbacks from the agent)
        let delegate = ClientDelegate::new(event_tx.clone(), name.clone());

        // Create the ACP protocol connection.
        // The Client trait is !Send, so we use tokio::task::spawn_local for the
        // internal spawn function. The caller must be inside a LocalSet.
        let (connection, io_future) =
            acp::ClientSideConnection::new(delegate, stdin_compat, stdout_compat, |fut| {
                tokio::task::spawn_local(fut);
            });

        let connection = std::rc::Rc::new(connection);
        let io_task = tokio::task::spawn_local({
            let agent_name = name.clone();
            async move {
                if let Err(e) = io_future.await {
                    tracing::warn!("Agent '{}' I/O task ended with error: {}", agent_name, e);
                }
            }
        });

        // Spawn stderr reader task — captures agent stderr and logs via tracing
        let stderr_task = tokio::spawn({
            let agent_name = name.clone();
            async move {
                let reader = tokio::io::BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!("[{} stderr] {}", agent_name, line);
                }
            }
        });

        // Spawn process monitor task — detects when the child exits and sends
        // an AgentExited event to the main loop.
        let monitor_task = tokio::spawn({
            let agent_name = name.clone();
            async move {
                let status = child.wait().await;
                match status {
                    Ok(exit_status) => {
                        let exit_code = exit_code_from_status(exit_status);
                        tracing::info!("Agent '{}' exited with code: {:?}", agent_name, exit_code);
                        let _ = event_tx.send(AgentEvent::AgentExited {
                            agent_name,
                            exit_code,
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to wait for agent '{}': {}", agent_name, e);
                        let _ = event_tx.send(AgentEvent::AgentExited {
                            agent_name,
                            exit_code: None,
                        });
                    }
                }
            }
        });

        Ok(Self {
            name,
            connection,
            _io_task: io_task,
            _stderr_task: stderr_task,
            _monitor_task: monitor_task,
            capabilities: None,
            auth_methods: Vec::new(),
        })
    }

    /// Send an ACP `InitializeRequest` and negotiate protocol capabilities.
    ///
    /// This must be called once after `spawn()` to complete the ACP handshake.
    /// On success, stores the agent's capabilities and auth methods.
    /// Returns an error if the agent's protocol version is below the minimum.
    pub async fn initialize(&mut self) -> Result<acp::InitializeResponse> {
        let version = env!("CARGO_PKG_VERSION");

        let request = acp::InitializeRequest::new(acp::ProtocolVersion::V1)
            .client_capabilities(
                acp::ClientCapabilities::new()
                    .fs(acp::FileSystemCapability::new()
                        .read_text_file(true)
                        .write_text_file(true))
                    .terminal(true),
            )
            .client_info(acp::Implementation::new("emacs-acp-proxy", version));

        let response = self
            .connection
            .initialize(request)
            .await
            .context("ACP initialize request failed")?;

        if response.protocol_version < MINIMUM_SUPPORTED_VERSION {
            bail!(
                "Agent '{}' protocol version {:?} is below minimum supported {:?}",
                self.name,
                response.protocol_version,
                MINIMUM_SUPPORTED_VERSION
            );
        }

        self.capabilities = Some(response.agent_capabilities.clone());
        self.auth_methods = response.auth_methods.clone();

        tracing::info!(
            "Agent '{}' initialized: version={:?}, auth_methods={}",
            self.name,
            response.protocol_version,
            response.auth_methods.len()
        );

        Ok(response)
    }
}

fn exit_code_from_status(status: ExitStatus) -> Option<i32> {
    status.code()
}
