use std::{sync::Arc, time::Duration};

use time::OffsetDateTime;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::watch,
    time::Instant,
};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{PluginId, PluginInstanceId, PluginName},
};

use super::SESSION_FAILURE_GRACE;

const OUTPUT_CHUNK_BYTES: usize = 8192;

pub(super) async fn pipe_plugin_output(
    mut stream: impl AsyncRead + Unpin,
    logger: Arc<NodeLogger>,
    plugin_id: PluginId,
    plugin_name: PluginName,
    instance_id: PluginInstanceId,
    stream_name: &'static str,
    correlation_id: String,
) {
    let mut buffer = vec![0_u8; OUTPUT_CHUNK_BYTES];
    loop {
        let count = match stream.read(&mut buffer).await {
            Ok(0) => return,
            Ok(count) => count,
            Err(_) => {
                logger.emit(
                    LogLevel::Warn,
                    "oll::plugin",
                    "plugin_output_read_failed",
                    &correlation_id,
                    serde_json::json!({
                        "plugin_id": plugin_id.as_str(),
                        "plugin_instance_id": instance_id.to_string(),
                        "stream": stream_name,
                        "error_code": "plugin_output_read_failed",
                    }),
                );
                return;
            }
        };
        let message = String::from_utf8_lossy(&buffer[..count]);
        let timestamp = OffsetDateTime::now_utc();
        logger.emit_plugin(
            LogLevel::Info,
            if stream_name == "stdout" {
                "plugin::stdout"
            } else {
                "plugin::stderr"
            },
            if stream_name == "stdout" {
                "plugin_stdout"
            } else {
                "plugin_stderr"
            },
            &correlation_id,
            timestamp,
            serde_json::json!({
                "plugin_id": plugin_id.as_str(),
                "plugin_name": plugin_name.as_str(),
                "plugin_instance_id": instance_id.to_string(),
                "stream": stream_name,
                "message": message.trim_end_matches(['\r', '\n']),
            }),
        );
    }
}

pub(super) async fn stop_server(
    shutdown: watch::Sender<bool>,
    mut server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    absolute_deadline: Instant,
) {
    shutdown.send_replace(true);
    let deadline = absolute_deadline.min(Instant::now() + SESSION_FAILURE_GRACE);
    if tokio::time::timeout_at(deadline, &mut server)
        .await
        .is_err()
    {
        server.abort();
        let _ = server.await;
    }
}

pub(super) async fn finish_output_tasks(
    stdout: Option<tokio::task::JoinHandle<()>>,
    stderr: Option<tokio::task::JoinHandle<()>>,
    absolute_deadline: Instant,
) {
    for mut task in [stdout, stderr].into_iter().flatten() {
        let deadline = absolute_deadline.min(Instant::now() + Duration::from_millis(100));
        if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
}
