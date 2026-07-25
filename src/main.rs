use std::process::ExitCode;

use clap::Parser;
use onelastleaf::cli::{Cli, Environment};

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = cli.validate() {
        error.exit();
    }

    match cli.execute_stage_gate(&Environment::from_process()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oll: {error}");
            error.exit_code()
        }
    }
}
