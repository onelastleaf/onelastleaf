use std::process::ExitCode;

use clap::Parser;
use onelastleaf::{
    cli::{Cli, EXIT_CONFIG, Environment},
    node,
};

fn main() -> ExitCode {
    let cli = Cli::parse();
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
    let prepared = match intent.prepare(&Environment::from_process(), &cwd) {
        Ok(prepared) => prepared,
        Err(error) => {
            eprintln!("oll: {error}");
            return error.exit_code();
        }
    };

    match node::execute(prepared) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("oll: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}
