use onelastleaf_plugin_sdk::{ActionResult, Plugin, SdkError};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    Plugin::builder("__OLL_PLUGIN_ID__", "0.1.0")
        .action("echo", "Return the supplied arguments", |_, arguments| async move {
            Ok(ActionResult {
                result: Some(__OLL_PLUGIN_MODULE__::echo(arguments)),
                artifacts: Vec::new(),
            })
        })?
        .build()?
        .run()
        .await
}
