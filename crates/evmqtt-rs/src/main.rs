use clap::Parser;
use evmqtt_rs::app;
use evmqtt_rs::config::Config;
use evmqtt_rs::discovery::write_detect_snippets;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "evmqtt-rs",
    version,
    about = "Bridge /dev/input/event* keys to per-key Home Assistant MQTT triggers"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "evmqtt-rs.toml")]
    config: PathBuf,

    /// Print ready-to-paste TOML `[[device]]` snippets for every visible
    /// input device and exit. The most precise matcher each device supports
    /// is suggested (unique_id, then bus_vendor_product_version, then name).
    #[arg(long)]
    detect: bool,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("EVMQTT_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,rumqttc=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn detect() -> ExitCode {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    match write_detect_snippets(&mut lock) {
        Ok(0) => {
            let _ = lock.flush();
            drop(lock);
            eprintln!(
                "no input devices were visible to this process. Check that \
                 the user has read access to /dev/input/event* (usually via \
                 the `input` group)."
            );
            ExitCode::from(1)
        }
        Ok(_) => {
            let _ = lock.flush();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("detect: write error: {e}");
            ExitCode::from(1)
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    if cli.detect {
        return detect();
    }

    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    match app::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fatal: {e}");
            ExitCode::from(1)
        }
    }
}
