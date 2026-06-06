pub mod agent;
pub mod application;
pub mod config;
pub mod connection;
pub mod main_loop;
pub mod msg;

use std::path::{Path, PathBuf};
use std::process;
use tracing_subscriber::fmt::time;

use config::Config;
use connection::Connection;
use tracing_subscriber::EnvFilter;

struct CliArgs {
    stdio: bool,
    config_path: Option<PathBuf>,
    log_level: Option<String>,
    log_file: Option<PathBuf>,
}

fn parse_args() -> CliArgs {
    let mut args = CliArgs {
        stdio: false,
        config_path: None,
        log_level: None,
        log_file: None,
    };

    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--stdio" => args.stdio = true,
            "--config" => {
                args.config_path = iter.next().map(PathBuf::from);
                if args.config_path.is_none() {
                    eprintln!("error: --config requires a path argument");
                    process::exit(1);
                }
            }
            "--log-level" => {
                args.log_level = iter.next();
                if args.log_level.is_none() {
                    eprintln!("error: --log-level requires a value (trace/debug/info/warn/error)");
                    process::exit(1);
                }
            }
            "--log-file" => {
                args.log_file = iter.next().map(PathBuf::from);
                if args.log_file.is_none() {
                    eprintln!("error: --log-file requires a path argument");
                    process::exit(1);
                }
            }
            other => {
                eprintln!("error: unknown argument: {other}");
                process::exit(1);
            }
        }
    }

    args
}

fn init_tracing(level: &str, log_file: Option<&Path>) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| {
        eprintln!("warning: invalid log level '{level}', falling back to 'info'");
        EnvFilter::new("info")
    });

    if let Some(path) = log_file {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::File::create(path) {
            Ok(file) => {
                tracing_subscriber::fmt()
                    .with_env_filter(filter)
                    .with_writer(file)
                    .with_ansi(false)
                    .with_timer(time::LocalTime::rfc_3339())
                    .init();
                return;
            }
            Err(e) => {
                eprintln!(
                    "warning: failed to open log file {}: {}, falling back to stderr",
                    path.display(),
                    e
                );
            }
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

fn main() {
    let cli = parse_args();

    if !cli.stdio {
        eprintln!("error: --stdio flag is required");
        process::exit(1);
    }

    // CLI --log-level takes precedence over config file, default is "info"
    let log_level = cli
        .log_level
        .as_deref()
        // .or(config.log_level.as_deref())
        .unwrap_or("info");

    init_tracing(log_level, cli.log_file.as_deref());

    // Load config (before tracing init so we can read log_level from config)
    let config = Config::load(cli.config_path.as_deref())
        .unwrap_or_else(|e| {
            eprintln!("warning: failed to load config: {e}, using defaults");
            Config::default()
        })
        .merge_with_defaults();

    tracing::info!("emacs-acp-proxy starting");
    tracing::info!("using config: {config:#?}");

    let (connection, io_threads) = Connection::stdio();

    if let Err(e) = main_loop::main_loop(connection, config) {
        tracing::error!("main loop error: {e:#}");
        process::exit(1);
    }

    if let Err(e) = io_threads.join() {
        tracing::error!("I/O thread error: {e}");
    }

    tracing::info!("emacs-acp-proxy exiting");
}
