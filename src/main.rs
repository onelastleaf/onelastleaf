use std::process::ExitCode;

use clap::Parser;
use onelastleaf::cli::{Cli, EXIT_CONFIG, EXIT_UNAVAILABLE, Environment};

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = cli.validate() {
        error.exit();
    }

    let intent = match cli.into_intent() {
        Ok(intent) => intent,
        Err(error) => error.exit(),
    };

    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => {
            eprintln!("oll: cannot determine process startup working directory: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let _prepared = match intent.prepare(&Environment::from_process(), &cwd) {
        Ok(prepared) => prepared,
        Err(error) => {
            eprintln!("oll: {error}");
            return error.exit_code();
        }
    };

    eprintln!("oll: command is not implemented");
    ExitCode::from(EXIT_UNAVAILABLE)
}
