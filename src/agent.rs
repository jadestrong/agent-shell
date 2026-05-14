//! Stored state for a connected ACP agent.

use agent_client_protocol::{ConnectionTo, role::acp::Agent};

/// A connected ACP agent and its negotiated metadata.
pub struct AgentConnection {
    /// ACP connection handle. Clone-able; backed by internal channels.
    pub connection: ConnectionTo<Agent>,
    /// Serialized agent capabilities from the initialize response.
    pub capabilities: serde_json::Value,
    /// Serialized auth methods from the initialize response.
    pub auth_methods: serde_json::Value,
}
