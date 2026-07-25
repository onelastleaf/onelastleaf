use std::process::ExitCode;

use clap::Parser;
use onelastleaf::cli::{Cli, EXIT_UNAVAILABLE, Environment};

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = cli.validate() {
        error.exit();
    }

    let mut intent = match cli.into_intent() {
        Ok(intent) => intent,
        Err(error) => error.exit(),
    };

    if let Err(error) = intent.resolve_client_paths_from_process() {
        eprintln!("oll: {error}");
        return error.exit_code();
    }

    if let Err(error) = intent.validate_environment(&Environment::from_process()) {
        eprintln!("oll: {error}");
        return error.exit_code();
    }

    eprintln!("oll: command is not implemented");
    ExitCode::from(EXIT_UNAVAILABLE)
}
