use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub log_level: Option<String>,
    #[serde(default)]
    pub agents: HashMap<String, AgentConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub env: Option<HashMap<String, String>>,
    pub default_mode: Option<String>,
    pub default_model: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: None,
            agents: Self::builtin_agents(),
        }
    }
}

impl Config {
    /// Built-in agent definitions. Currently includes Claude Code via
    /// `@zed-industries/claude-agent-acp`.
    fn builtin_agents() -> HashMap<String, AgentConfig> {
        let mut agents = HashMap::new();
        agents.insert(
            "claude".to_string(),
            AgentConfig {
                command: "claude-agent-acp".to_string(),
                args: vec![],
                env: None,
                default_mode: None,
                default_model: None,
            },
        );
        agents
    }
}

impl Config {
    /// Returns the default config file path: `~/.config/emacs-acp-proxy/agents.toml`
    pub fn default_path() -> Option<PathBuf> {
        dirs::config_dir().map(|p| p.join("emacs-acp-proxy").join("agents.toml"))
    }

    /// Load configuration from a file path. If `path` is None, uses the default path.
    /// Returns an empty default config with a warning if the file doesn't exist or can't be parsed.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let config_path = match path {
            Some(p) => p.to_path_buf(),
            None => match Self::default_path() {
                Some(p) => p,
                None => {
                    tracing::warn!(
                        "Could not determine default config directory, using empty config"
                    );
                    return Ok(Self::default());
                }
            },
        };

        tracing::info!("Loading config from {}", config_path.display());

        match std::fs::read_to_string(&config_path) {
            Ok(contents) => match toml::from_str::<Config>(&contents) {
                Ok(config) => Ok(config),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse config file {}: {}, using empty config",
                        config_path.display(),
                        e
                    );
                    Ok(Self::default())
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    "Config file not found at {}, using empty config",
                    config_path.display()
                );
                Ok(Self::default())
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to read config file {}: {}, using empty config",
                    config_path.display(),
                    e
                );
                Ok(Self::default())
            }
        }
    }

    /// Merge this config with built-in defaults. User config values take priority.
    pub fn merge_with_defaults(self) -> Self {
        let defaults = Self::default();

        Self {
            log_level: self.log_level.or(defaults.log_level),
            agents: {
                let mut merged = defaults.agents;
                // User config agents override defaults
                merged.extend(self.agents);
                merged
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_valid_config() {
        let toml_content = r#"
log_level = "debug"

[agents.claude]
command = "node"
args = ["index.js"]
default_model = "claude-sonnet-4-20250514"

[agents.custom]
command = "/usr/bin/my-agent"
args = ["--stdio"]
env = { "API_KEY" = "test-key" }
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path())).unwrap();

        assert_eq!(config.log_level, Some("debug".to_string()));
        assert_eq!(config.agents.len(), 2);

        let claude = &config.agents["claude"];
        assert_eq!(claude.command, "node");
        assert_eq!(claude.args, vec!["index.js"]);
        assert_eq!(
            claude.default_model,
            Some("claude-sonnet-4-20250514".to_string())
        );
        assert!(claude.env.is_none());

        let custom = &config.agents["custom"];
        assert_eq!(custom.command, "/usr/bin/my-agent");
        assert_eq!(custom.args, vec!["--stdio"]);
        let env = custom.env.as_ref().unwrap();
        assert_eq!(env["API_KEY"], "test-key");
    }

    #[test]
    fn test_default_config_includes_claude() {
        let config = Config::default();
        assert!(config.agents.contains_key("claude"));
        let claude = &config.agents["claude"];
        assert_eq!(claude.command, "claude-agent-acp");
        assert!(claude.args.is_empty());
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let config = Config::load(Some(Path::new("/nonexistent/path/agents.toml"))).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_load_invalid_toml_returns_default() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"this is not valid toml [[[").unwrap();

        let config = Config::load(Some(file.path())).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_load_empty_file_returns_no_user_agents() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"").unwrap();

        let config = Config::load(Some(file.path())).unwrap();
        // Empty file parses to empty agents (no user-defined agents)
        assert!(config.agents.is_empty());
        assert!(config.log_level.is_none());
        // After merge, built-in claude agent appears
        let merged = config.merge_with_defaults();
        assert!(merged.agents.contains_key("claude"));
    }

    #[test]
    fn test_load_partial_config() {
        let toml_content = r#"
[agents.claude]
command = "node"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path())).unwrap();
        assert_eq!(config.agents.len(), 1);

        let claude = &config.agents["claude"];
        assert_eq!(claude.command, "node");
        assert!(claude.args.is_empty());
        assert!(claude.env.is_none());
        assert!(claude.default_mode.is_none());
        assert!(claude.default_model.is_none());
    }

    #[test]
    fn test_merge_with_defaults_user_overrides() {
        let user_config = Config {
            log_level: Some("trace".to_string()),
            agents: {
                let mut m = HashMap::new();
                m.insert(
                    "claude".to_string(),
                    AgentConfig {
                        command: "my-node".to_string(),
                        args: vec!["custom.js".to_string()],
                        env: None,
                        default_mode: None,
                        default_model: Some("my-model".to_string()),
                    },
                );
                m
            },
        };

        let merged = user_config.merge_with_defaults();

        assert_eq!(merged.log_level, Some("trace".to_string()));
        // User's claude overrides the built-in claude
        assert_eq!(merged.agents["claude"].command, "my-node");
        assert_eq!(
            merged.agents["claude"].default_model,
            Some("my-model".to_string())
        );
    }

    #[test]
    fn test_merge_empty_user_config_gets_builtin_claude() {
        let user_config = Config {
            log_level: None,
            agents: HashMap::new(),
        };
        let merged = user_config.merge_with_defaults();
        // Built-in claude should be present
        assert!(merged.agents.contains_key("claude"));
        assert_eq!(merged.agents["claude"].command, "claude-agent-acp");
    }

    #[test]
    fn test_config_toml_roundtrip() {
        let config = Config {
            log_level: Some("info".to_string()),
            agents: {
                let mut m = HashMap::new();
                m.insert(
                    "test-agent".to_string(),
                    AgentConfig {
                        command: "test-cmd".to_string(),
                        args: vec!["--flag".to_string()],
                        env: Some({
                            let mut e = HashMap::new();
                            e.insert("KEY".to_string(), "VALUE".to_string());
                            e
                        }),
                        default_mode: Some("fast".to_string()),
                        default_model: Some("model-v1".to_string()),
                    },
                );
                m
            },
        };

        let toml_str = toml::to_string(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(config, parsed);
    }
}
