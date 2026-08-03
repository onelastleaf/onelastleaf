use std::{
    env,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::CommandFactory;

use crate::configuration::{ReplicaStoreConfig, ResolvedNodeConfig};

use super::*;

fn parse(arguments: &[&str]) -> Cli {
    parse_from(arguments).unwrap()
}

fn intent(arguments: &[&str]) -> CliIntent {
    parse(arguments).into_intent().unwrap()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            env::temp_dir().join(format!("oll-cli-unit-test-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn write_config(&self, source: &str) -> PathBuf {
        let root = self.0.join("config");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("config.lua"), source).unwrap();
        root
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

mod parsing;
mod plugin;
mod preparation;
mod replica;
mod sync;
mod validation;
