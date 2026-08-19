#[path = "plugin_sdk_conformance/session.rs"]
mod session;

use std::{env, path::PathBuf};

use session::{FixtureCommand, run_conformance};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OLL_SDK_CONFORMANCE_PROGRAM to name a built SDK fixture"]
async fn external_sdk_obeys_the_plugin_protocol() {
    let program = env::var_os("OLL_SDK_CONFORMANCE_PROGRAM")
        .map(PathBuf::from)
        .expect("OLL_SDK_CONFORMANCE_PROGRAM is required");
    let arguments = env::var("OLL_SDK_CONFORMANCE_ARGS")
        .ok()
        .map(|value| serde_json::from_str::<Vec<String>>(&value).expect("invalid JSON argv"))
        .unwrap_or_default();
    let working_directory = env::var_os("OLL_SDK_CONFORMANCE_CWD").map(PathBuf::from);

    run_conformance(FixtureCommand {
        program,
        arguments,
        working_directory,
    })
    .await
    .unwrap();
}
