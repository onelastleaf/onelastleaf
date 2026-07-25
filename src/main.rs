use std::process::ExitCode;

use clap::Parser;
use onelastleaf::cli::{Cli, EXIT_UNAVAILABLE, Environment};

fn main() -> ExitCode {
    let mut cli = Cli::parse();
    if let Err(error) = cli.validate() {
        error.exit();
    }

    if let Err(error) = cli.resolve_client_paths_from_process() {
        eprintln!("oll: {error}");
        return error.exit_code();
    }

    if let Err(error) = cli.validate_environment(&Environment::from_process()) {
        eprintln!("oll: {error}");
        return error.exit_code();
    }

    eprintln!("oll: command is not implemented");
    ExitCode::from(EXIT_UNAVAILABLE)
}
