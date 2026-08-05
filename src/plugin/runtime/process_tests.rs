use std::{
    fs, io,
    pin::Pin,
    process::Stdio,
    task::{Context, Poll},
    time::Duration,
};

use tempfile::TempDir;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, transport::Endpoint};

use crate::{
    node::{NodeIdentity, logging::NodeLogger},
    plugin::PluginId,
    protocol::oll::{self, plugin_runtime_client::PluginRuntimeClient},
};

use super::*;
use super::{output::pipe_plugin_output, termination::process_group_exists};

struct FailingOutput;

#[tokio::test]
async fn plugin_service_rejects_an_oversized_envelope_before_dispatch() {
    const TEST_LIMIT: usize = 1024;
    assert_eq!(MAX_PLUGIN_GRPC_MESSAGE_BYTES, 64 * 1024 * 1024);
    const { assert!(MAX_PLUGIN_GRPC_MESSAGE_BYTES < usize::MAX) };

    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (service, connection) = InstanceService::new();
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        Server::builder()
            .add_service(plugin_runtime_service_with_limit(service, TEST_LIMIT))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            }),
    );

    let channel = Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let (outgoing, receiver) = tokio::sync::mpsc::channel(1);
    let mut client = PluginRuntimeClient::new(channel)
        .max_encoding_message_size(TEST_LIMIT * 4)
        .max_decoding_message_size(TEST_LIMIT * 4);
    let _host_messages = client
        .connect(Request::new(ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    let mut connected = connection.await.unwrap();
    outgoing
        .send(oll::PluginEnvelope {
            message_id: 1,
            reply_to: None,
            session_id: "oversized-envelope".to_owned(),
            plugin_instance_id: PluginInstanceId::new().to_string(),
            trace: None,
            payload: Some(oll::plugin_envelope::Payload::Log(oll::LogRecord {
                message: "x".repeat(TEST_LIMIT * 2),
                ..Default::default()
            })),
        })
        .await
        .unwrap();

    let error = connected.incoming.message().await.unwrap_err();
    assert!(matches!(
        error.code(),
        tonic::Code::OutOfRange | tonic::Code::ResourceExhausted
    ));
    drop(connected);
    let _ = shutdown.send(());
    server.await.unwrap().unwrap();
}

impl AsyncRead for FailingOutput {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("secret device failure")))
    }
}

#[tokio::test]
async fn output_read_failure_is_logged_with_stable_redacted_context() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(
        &logs,
        NodeIdentity::generate("runtime-test".parse().unwrap()),
    )
    .unwrap();
    let plugin_id: PluginId = "oll.runtime-test".parse().unwrap();
    let instance_id = PluginInstanceId::new();

    pipe_plugin_output(
        FailingOutput,
        logger.clone(),
        plugin_id.clone(),
        "runtime-test".parse().unwrap(),
        instance_id,
        "stderr",
        "output-read-correlation".to_owned(),
    )
    .await;
    logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();

    let records = fs::read_to_string(logs.join("oll.log")).unwrap();
    let record: serde_json::Value = serde_json::from_str(records.trim()).unwrap();
    assert_eq!(record["event"], "plugin_output_read_failed");
    assert_eq!(record["level"], "WARN");
    assert_eq!(record["correlation_id"], "output-read-correlation");
    assert_eq!(record["plugin_id"], plugin_id.as_str());
    assert_eq!(record["plugin_instance_id"], instance_id.to_string());
    assert_eq!(record["stream"], "stderr");
    assert_eq!(record["error_code"], "plugin_output_read_failed");
    assert!(!records.contains("secret device failure"));
}

#[tokio::test]
async fn stubborn_process_group_is_reaped_within_one_absolute_deadline() {
    let directory = TempDir::new().unwrap();
    let logger = NodeLogger::open(
        &directory.path().join("logs"),
        NodeIdentity::generate("runtime-test".parse().unwrap()),
    )
    .unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", "trap '' TERM; while :; do sleep 1; done"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    let process_id = child.id().unwrap();
    let mut process = OwnedPluginProcess {
        child,
        process_group: i32::try_from(process_id).unwrap(),
        reaped: false,
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let plugin_id: PluginId = "oll.runtime-test".parse().unwrap();
    let instance_id = PluginInstanceId::new();
    let absolute_deadline = Instant::now() + Duration::from_millis(900);
    terminate_process(
        &mut process,
        Instant::now() + Duration::from_millis(50),
        absolute_deadline,
        &logger,
        &plugin_id,
        instance_id,
        "stubborn-process-test",
    )
    .await
    .unwrap();

    assert!(process.reaped);
    assert!(Instant::now() < absolute_deadline);
    logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
}

#[tokio::test]
async fn reaped_leader_does_not_leave_its_descendant_process_group_alive() {
    let directory = TempDir::new().unwrap();
    let logger = NodeLogger::open(
        &directory.path().join("logs"),
        NodeIdentity::generate("runtime-test".parse().unwrap()),
    )
    .unwrap();
    let descendant_pid_path = directory.path().join("descendant.pid");
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-c",
            "(trap '' TERM; while :; do sleep 1; done) & echo $! > \"$DESCENDANT_PID_PATH\"; exit 0",
        ])
        .env("DESCENDANT_PID_PATH", &descendant_pid_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    let process_id = child.id().unwrap();
    let process_group = i32::try_from(process_id).unwrap();
    let mut process = OwnedPluginProcess {
        child,
        process_group,
        reaped: false,
    };
    let pid_deadline = Instant::now() + Duration::from_secs(1);
    while !descendant_pid_path.exists() {
        assert!(
            Instant::now() < pid_deadline,
            "leader did not publish its descendant PID"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let plugin_id: PluginId = "oll.runtime-test".parse().unwrap();
    terminate_process(
        &mut process,
        Instant::now(),
        Instant::now() + Duration::from_secs(3),
        &logger,
        &plugin_id,
        PluginInstanceId::new(),
        "reaped-leader-test",
    )
    .await
    .unwrap();

    assert!(process.reaped);
    assert!(!process_group_exists(process_group).unwrap());
}
