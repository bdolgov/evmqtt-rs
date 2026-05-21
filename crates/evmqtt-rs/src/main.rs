use clap::Parser;
use evmqtt_rs::config::{Args, Mode, Runtime};
use evmqtt_rs::{cli, daemon};
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let filter = EnvFilter::try_from_env("EVMQTT_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,rumqttc=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .compact()
        .init();
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let args = Args::parse();
    let runtime = match args.into_runtime() {
        Ok(r) => r,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };
    let Runtime {
        mqtt,
        hass,
        db_path,
        mode,
    } = runtime;

    match mode {
        Mode::Daemon => match daemon::run(mqtt, hass, db_path).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("fatal: {e}");
                ExitCode::from(1)
            }
        },
        Mode::ListDevices => match cli::list_devices(&mqtt).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::from(1)
            }
        },
        Mode::Manage {
            enable,
            disable,
            remove,
        } => {
            let mut rc = ExitCode::SUCCESS;
            if !enable.is_empty()
                && let Err(e) = cli::enable_devices(&mqtt, &enable).await
            {
                eprintln!("error: {e}");
                rc = ExitCode::from(1);
            }
            if !disable.is_empty()
                && let Err(e) = cli::disable_devices(&mqtt, &disable).await
            {
                eprintln!("error: {e}");
                rc = ExitCode::from(1);
            }
            if !remove.is_empty()
                && let Err(e) = cli::remove_devices(&mqtt, &remove).await
            {
                eprintln!("error: {e}");
                rc = ExitCode::from(1);
            }
            rc
        }
    }
}
