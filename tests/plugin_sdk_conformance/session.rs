use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    time::Duration,
};

use futures_util::Stream;
use onelastleaf::protocol::{PROTOCOL_SCHEMA_SHA256, oll, oll::plugin_envelope};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use uuid::Uuid;

const PLUGIN_ID: &str = "org.onelastleaf.conformance";
const PLUGIN_NAME: &str = "conformance-fixture";
const SESSION_ID: &str = "sdk-conformance-session";
const INSTANCE_ID: &str = "sdk-conformance-instance";
const MAXIMUM_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_ARTIFACT_CHUNK_BYTES: u64 = 64 * 1024;
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

type OutboundStream =
    Pin<Box<dyn Stream<Item = Result<oll::PluginEnvelope, Status>> + Send + 'static>>;

#[derive(Clone)]
pub(super) struct FixtureCommand {
    pub(super) program: PathBuf,
    pub(super) arguments: Vec<String>,
    pub(super) working_directory: Option<PathBuf>,
}

struct ConnectedSession {
    incoming: Streaming<oll::PluginEnvelope>,
    outgoing: mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
}

struct FixtureService {
    connection: Mutex<Option<oneshot::Sender<ConnectedSession>>>,
}

#[tonic::async_trait]
impl oll::plugin_runtime_server::PluginRuntime for FixtureService {
    type ConnectStream = OutboundStream;

    async fn connect(
        &self,
        request: Request<Streaming<oll::PluginEnvelope>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let connection = self
            .connection
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::already_exists("fixture already connected"))?;
        let (outgoing, receiver) = mpsc::channel(256);
        connection
            .send(ConnectedSession {
                incoming: request.into_inner(),
                outgoing,
            })
            .map_err(|_| Status::unavailable("conformance driver stopped"))?;
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

struct Driver {
    connection: ConnectedSession,
    next_message_id: u64,
    last_plugin_message_id: u64,
}

struct RunningFixture {
    child: Child,
    driver: Driver,
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Driver {
    async fn send(
        &mut self,
        correlation_id: &str,
        reply_to: Option<u64>,
        payload: plugin_envelope::Payload,
    ) -> Result<u64, String> {
        self.send_with_identity(correlation_id, reply_to, SESSION_ID, INSTANCE_ID, payload)
            .await
    }

    async fn send_with_identity(
        &mut self,
        correlation_id: &str,
        reply_to: Option<u64>,
        session_id: &str,
        instance_id: &str,
        payload: plugin_envelope::Payload,
    ) -> Result<u64, String> {
        let message_id = self.next_message_id;
        self.next_message_id = self
            .next_message_id
            .checked_add(1)
            .ok_or_else(|| "host exhausted message IDs".to_owned())?;
        let envelope = oll::PluginEnvelope {
            message_id,
            reply_to,
            session_id: session_id.to_owned(),
            plugin_instance_id: instance_id.to_owned(),
            trace: Some(trace(correlation_id)),
            payload: Some(payload),
        };
        if envelope.encoded_len() > MAXIMUM_ENVELOPE_BYTES {
            return Err("host attempted to send an oversized envelope".to_owned());
        }
        self.connection
            .outgoing
            .send(Ok(envelope))
            .await
            .map_err(|_| "plugin closed the response stream".to_owned())?;
        Ok(message_id)
    }

    async fn receive(&mut self, correlation_id: &str) -> Result<oll::PluginEnvelope, String> {
        let envelope = timeout(STEP_TIMEOUT, self.connection.incoming.message())
            .await
            .map_err(|_| "timed out waiting for a plugin envelope".to_owned())?
            .map_err(|error| format!("plugin stream failed: {error}"))?
            .ok_or_else(|| "plugin closed its request stream".to_owned())?;
        if envelope.encoded_len() > MAXIMUM_ENVELOPE_BYTES {
            return Err("plugin sent an oversized envelope".to_owned());
        }
        if envelope.message_id == 0 || envelope.message_id <= self.last_plugin_message_id {
            return Err("plugin message IDs are not strictly increasing".to_owned());
        }
        self.last_plugin_message_id = envelope.message_id;
        if envelope.session_id != SESSION_ID || envelope.plugin_instance_id != INSTANCE_ID {
            return Err("plugin changed the negotiated session identity".to_owned());
        }
        let trace = envelope
            .trace
            .as_ref()
            .ok_or_else(|| "plugin omitted trace context".to_owned())?;
        if trace.correlation_id != correlation_id {
            return Err("plugin changed correlation context".to_owned());
        }
        Ok(envelope)
    }
}

pub(super) async fn run_conformance(command: FixtureCommand) -> Result<(), String> {
    invalid_endpoint_is_rejected(command.clone())
        .await
        .map_err(|error| format!("invalid endpoint: {error}"))?;
    run_protocol_session(command.clone())
        .await
        .map_err(|error| format!("protocol session: {error}"))?;
    stale_session_is_rejected(command.clone())
        .await
        .map_err(|error| format!("stale session: {error}"))?;
    oversized_envelope_is_rejected(command.clone())
        .await
        .map_err(|error| format!("receive limit: {error}"))?;
    response_stream_failure_stops_runtime(command.clone())
        .await
        .map_err(|error| format!("stream failure: {error}"))?;
    parent_eof_stops_runtime(command)
        .await
        .map_err(|error| format!("parent liveness: {error}"))
}

async fn run_protocol_session(command: FixtureCommand) -> Result<(), String> {
    let RunningFixture {
        mut child,
        driver,
        shutdown,
        server,
    } = start_connected(command).await?;
    let mut driver = driver;
    let result = exercise_protocol(&mut driver).await;
    if result.is_err() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let fixture_status_before_cleanup = child
        .try_wait()
        .map_err(|error| format!("inspect fixture after protocol exercise: {error}"))?;
    if result.is_err() {
        let _ = child.start_kill();
    }
    drop(driver);
    let parent_liveness = child.stdin.take();
    let status = timeout(STEP_TIMEOUT, child.wait()).await;
    if status.is_err() {
        let _ = child.start_kill();
    }
    drop(parent_liveness);
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| format!("wait for fixture: {error}"))?;
    stop_server(shutdown, server).await?;
    status
        .map_err(|_| {
            format!(
                "fixture did not exit within ten seconds\nfixture stdout:\n{}\nfixture stderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })?
        .map_err(|error| format!("wait for fixture: {error}"))?;
    if let Err(error) = result {
        return Err(format!(
            "{error}\nfixture status before cleanup: {fixture_status_before_cleanup:?}\nfixture stdout:\n{}\nfixture stderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if !output.status.success() {
        return Err(format!(
            "fixture exited with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

async fn start_connected(command: FixtureCommand) -> Result<RunningFixture, String> {
    if !command.program.is_absolute() {
        return Err("fixture program must be an absolute path".to_owned());
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| format!("bind fixture server: {error}"))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("inspect fixture listener: {error}"))?;
    let (connected, connection) = oneshot::channel();
    let service = FixtureService {
        connection: Mutex::new(Some(connected)),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(oll::plugin_runtime_server::PluginRuntimeServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let mut child = spawn_fixture(command, format!("http://{address}"))?;
    let connected = match timeout(STEP_TIMEOUT, connection).await {
        Ok(Ok(connected)) => connected,
        result => {
            let reason = match result {
                Err(_) => "fixture did not connect within ten seconds".to_owned(),
                Ok(Err(_)) => "fixture server dropped its connection slot".to_owned(),
                Ok(Ok(_)) => unreachable!(),
            };
            let _ = child.start_kill();
            let output = child
                .wait_with_output()
                .await
                .map_err(|error| format!("{reason}; wait for fixture: {error}"))?;
            stop_server(shutdown_tx, server).await?;
            return Err(format!(
                "{reason}\nfixture exited with {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    };
    Ok(RunningFixture {
        child,
        driver: Driver {
            connection: connected,
            next_message_id: 1,
            last_plugin_message_id: 0,
        },
        shutdown: shutdown_tx,
        server,
    })
}

async fn invalid_endpoint_is_rejected(command: FixtureCommand) -> Result<(), String> {
    let child = spawn_fixture(command, "https://127.0.0.1:1".to_owned())?;
    let output = timeout(STEP_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| "fixture did not reject an invalid endpoint".to_owned())?
        .map_err(|error| format!("wait for invalid-endpoint fixture: {error}"))?;
    if output.status.success() {
        return Err(format!(
            "fixture accepted an invalid endpoint\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

async fn stale_session_is_rejected(command: FixtureCommand) -> Result<(), String> {
    let mut fixture = start_connected(command).await?;
    handshake(&mut fixture.driver).await?;
    fixture
        .driver
        .send_with_identity(
            "00000000-0000-4000-8000-000000000060",
            None,
            "stale-session",
            INSTANCE_ID,
            plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: 60 }),
        )
        .await?;
    expect_fixture_exit(fixture, "stale session envelope").await
}

async fn oversized_envelope_is_rejected(command: FixtureCommand) -> Result<(), String> {
    let mut fixture = start_connected(command).await?;
    handshake(&mut fixture.driver).await?;
    let message_id = fixture.driver.next_message_id;
    fixture.driver.next_message_id = message_id
        .checked_add(1)
        .ok_or_else(|| "host exhausted message IDs".to_owned())?;
    let envelope = oll::PluginEnvelope {
        message_id,
        reply_to: None,
        session_id: SESSION_ID.to_owned(),
        plugin_instance_id: INSTANCE_ID.to_owned(),
        trace: Some(trace("00000000-0000-4000-8000-000000000061")),
        payload: Some(plugin_envelope::Payload::ProtocolError(
            oll::ProtocolError {
                code: oll::ErrorCode::Internal as i32,
                message: "x".repeat(MAXIMUM_ENVELOPE_BYTES),
                retryable: false,
                ..Default::default()
            },
        )),
    };
    if envelope.encoded_len() <= MAXIMUM_ENVELOPE_BYTES {
        return Err("oversized conformance envelope was not oversized".to_owned());
    }
    fixture
        .driver
        .connection
        .outgoing
        .send(Ok(envelope))
        .await
        .map_err(|_| "plugin closed before the oversized-envelope test".to_owned())?;
    expect_fixture_exit(fixture, "oversized envelope").await
}

async fn response_stream_failure_stops_runtime(command: FixtureCommand) -> Result<(), String> {
    let mut fixture = start_connected(command).await?;
    handshake(&mut fixture.driver).await?;
    let RunningFixture {
        child,
        driver,
        shutdown,
        server,
    } = fixture;
    drop(driver.connection.outgoing);
    expect_child_exit(child, shutdown, server, "closed host response stream", None).await
}

async fn parent_eof_stops_runtime(command: FixtureCommand) -> Result<(), String> {
    let mut fixture = start_connected(command).await?;
    drop(fixture.child.stdin.take());
    expect_fixture_exit(fixture, "parent-liveness EOF").await
}

async fn expect_fixture_exit(fixture: RunningFixture, trigger: &str) -> Result<(), String> {
    let RunningFixture {
        child,
        driver,
        shutdown,
        server,
    } = fixture;
    expect_child_exit(child, shutdown, server, trigger, Some(driver)).await
}

async fn expect_child_exit(
    mut child: Child,
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    trigger: &str,
    keepalive: Option<Driver>,
) -> Result<(), String> {
    // Tokio's wait helpers close a child stdin pipe before waiting. Keep the
    // liveness pipe outside Child so this test observes the requested trigger,
    // not an accidental EOF introduced by the harness itself.
    let parent_liveness = child.stdin.take();
    let status = timeout(STEP_TIMEOUT, child.wait()).await;
    if status.is_err() {
        let _ = child.start_kill();
    }
    drop(parent_liveness);
    drop(keepalive);
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| format!("wait for fixture after {trigger}: {error}"))?;
    stop_server(shutdown, server).await?;
    status
        .map_err(|_| {
            format!(
                "fixture did not exit after {trigger}\nfixture stdout:\n{}\nfixture stderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })?
        .map_err(|error| format!("wait for fixture after {trigger}: {error}"))?;
    Ok(())
}

async fn stop_server(
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
) -> Result<(), String> {
    let _ = shutdown.send(());
    server
        .await
        .map_err(|error| format!("fixture server task failed: {error}"))?
        .map_err(|error| format!("fixture server failed: {error}"))
}

fn spawn_fixture(command: FixtureCommand, endpoint: String) -> Result<Child, String> {
    let mut process = Command::new(&command.program);
    process
        .args(command.arguments)
        .env("OLL_PLUGIN_ENDPOINT", endpoint)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(directory) = command.working_directory {
        process.current_dir(directory);
    }
    process
        .spawn()
        .map_err(|error| format!("spawn fixture {}: {error}", command.program.display()))
}

async fn exercise_protocol(driver: &mut Driver) -> Result<(), String> {
    handshake(driver)
        .await
        .map_err(|error| format!("handshake: {error}"))?;
    heartbeat_and_cancellation(driver)
        .await
        .map_err(|error| format!("heartbeat/cancellation: {error}"))?;
    concurrent_echo_jobs(driver)
        .await
        .map_err(|error| format!("concurrent echo: {error}"))?;
    host_calls_and_log(driver)
        .await
        .map_err(|error| format!("host calls: {error}"))?;
    artifact_transfer(driver)
        .await
        .map_err(|error| format!("artifact: {error}"))?;
    shutdown(driver)
        .await
        .map_err(|error| format!("shutdown: {error}"))
}

async fn handshake(driver: &mut Driver) -> Result<(), String> {
    let correlation = "00000000-0000-4000-8000-000000000001";
    driver
        .send(
            correlation,
            None,
            plugin_envelope::Payload::HostHello(oll::HostHello {
                node: Some(oll::NodeIdentity {
                    node_id: Some(oll::NodeId {
                        value: "00000000-0000-4000-8000-000000000002".to_owned(),
                    }),
                    node_name: Some(oll::NodeName {
                        value: "conformance-host".to_owned(),
                    }),
                }),
                protocol_schema_sha256: PROTOCOL_SCHEMA_SHA256.to_vec(),
                maximum_call_depth: 8,
                maximum_causal_depth: 8,
                maximum_artifact_chunk_bytes: MAXIMUM_ARTIFACT_CHUNK_BYTES,
                plugin_id: Some(oll::PluginId {
                    value: PLUGIN_ID.to_owned(),
                }),
                plugin_name: Some(oll::PluginName {
                    value: PLUGIN_NAME.to_owned(),
                }),
            }),
        )
        .await?;
    let hello = driver.receive(correlation).await?;
    if hello.reply_to.is_some() {
        return Err("PluginHello unexpectedly set reply_to".to_owned());
    }
    let plugin_envelope::Payload::PluginHello(hello) = required_payload(hello)? else {
        return Err("first plugin payload is not PluginHello".to_owned());
    };
    if hello.plugin_id.as_ref().map(|value| value.value.as_str()) != Some(PLUGIN_ID)
        || hello.plugin_name.as_ref().map(|value| value.value.as_str()) != Some(PLUGIN_NAME)
        || hello.protocol_schema_sha256 != PROTOCOL_SCHEMA_SHA256
        || hello.plugin_version.is_empty()
    {
        return Err("PluginHello does not echo the negotiated identity".to_owned());
    }
    let actions = hello
        .actions
        .iter()
        .map(|action| action.name.as_str())
        .collect::<HashSet<_>>();
    for required in ["echo", "wait", "host", "artifact"] {
        if !actions.contains(required) {
            return Err(format!("fixture omitted required action `{required}`"));
        }
    }
    driver
        .send(
            correlation,
            None,
            plugin_envelope::Payload::Ready(oll::SessionReady {}),
        )
        .await?;
    let ready = driver.receive(correlation).await?;
    if ready.reply_to.is_some()
        || !matches!(required_payload(ready)?, plugin_envelope::Payload::Ready(_))
    {
        return Err("plugin did not complete SessionReady".to_owned());
    }
    Ok(())
}

async fn heartbeat_and_cancellation(driver: &mut Driver) -> Result<(), String> {
    let correlation = "00000000-0000-4000-8000-000000000010";
    let job_id = "00000000-0000-4000-8000-000000000011";
    let start_id = start_job(driver, correlation, job_id, "wait", &[]).await?;
    expect_job_accepted(driver, correlation, start_id, job_id).await?;

    let heartbeat_id = driver
        .send(
            correlation,
            None,
            plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: 42 }),
        )
        .await?;
    let heartbeat = driver.receive(correlation).await?;
    if heartbeat.reply_to != Some(heartbeat_id)
        || !matches!(
            required_payload(heartbeat)?,
            plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: 42 })
        )
    {
        return Err("heartbeat response changed nonce or reply_to".to_owned());
    }

    let cancel_id = driver
        .send(
            correlation,
            None,
            plugin_envelope::Payload::CancelJob(oll::CancelJobRequest {
                job_id: Some(oll::PluginJobId {
                    value: job_id.to_owned(),
                }),
                reason: oll::JobCancellationReason::UserRequest as i32,
            }),
        )
        .await?;
    let cancelled = driver.receive(correlation).await?;
    let plugin_envelope::Payload::CancelJobAcknowledged(acknowledged) =
        required_payload(cancelled.clone())?
    else {
        return Err("plugin did not acknowledge job cancellation".to_owned());
    };
    if cancelled.reply_to != Some(cancel_id)
        || acknowledged
            .job_id
            .as_ref()
            .map(|value| value.value.as_str())
            != Some(job_id)
    {
        return Err("cancellation acknowledgement names another request".to_owned());
    }
    Ok(())
}

async fn concurrent_echo_jobs(driver: &mut Driver) -> Result<(), String> {
    let jobs = (0_u64..32)
        .map(|index| {
            let correlation = format!("00000000-0000-4000-8000-{:012x}", 0x100 + index * 2);
            let job_id = format!("00000000-0000-4000-8000-{:012x}", 0x101 + index * 2);
            let expected = format!("value-{index}");
            (correlation, job_id, vec![expected.clone()], expected)
        })
        .collect::<Vec<_>>();
    let mut starts = HashMap::new();
    for (correlation, job_id, arguments, _) in &jobs {
        let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
        let id = start_job(driver, correlation, job_id, "echo", &arguments).await?;
        starts.insert(job_id.as_str(), (correlation.as_str(), id));
    }
    let mut accepted = HashSet::new();
    let mut completed = HashSet::new();
    while accepted.len() < jobs.len() || completed.len() < jobs.len() {
        let correlation = jobs
            .iter()
            .find(|(_, job_id, _, _)| !completed.contains(job_id.as_str()))
            .map(|value| value.0.as_str())
            .unwrap_or(jobs[0].0.as_str());
        let envelope = timeout(STEP_TIMEOUT, driver.connection.incoming.message())
            .await
            .map_err(|_| "timed out waiting for concurrent jobs".to_owned())?
            .map_err(|error| format!("plugin stream failed: {error}"))?
            .ok_or_else(|| "plugin closed during concurrent jobs".to_owned())?;
        validate_unscoped_receive(driver, &envelope)?;
        let actual_correlation = envelope
            .trace
            .as_ref()
            .map(|trace| trace.correlation_id.as_str())
            .unwrap_or(correlation);
        let payload = required_payload(envelope.clone())?;
        match payload {
            plugin_envelope::Payload::JobAccepted(value) => {
                let job_id = value
                    .job_id
                    .ok_or_else(|| "JobAccepted omitted JobId".to_owned())?
                    .value;
                let (expected_correlation, start_id) = starts
                    .get(job_id.as_str())
                    .ok_or_else(|| "JobAccepted names an unknown job".to_owned())?;
                if actual_correlation != *expected_correlation
                    || envelope.reply_to != Some(*start_id)
                {
                    return Err("JobAccepted changed routing context".to_owned());
                }
                accepted.insert(job_id);
            }
            plugin_envelope::Payload::JobUpdate(value) => {
                let job_id = value
                    .job_id
                    .as_ref()
                    .ok_or_else(|| "JobUpdate omitted JobId".to_owned())?
                    .value
                    .as_str();
                let expected = jobs
                    .iter()
                    .find(|(_, expected_job, _, _)| expected_job == job_id)
                    .ok_or_else(|| "JobUpdate names an unknown job".to_owned())?;
                if actual_correlation != expected.0
                    || value.state != oll::JobState::Succeeded as i32
                    || value.result.as_ref().and_then(config_string) != Some(expected.3.as_str())
                {
                    return Err("echo job returned the wrong terminal result".to_owned());
                }
                completed.insert(job_id.to_owned());
            }
            _ => return Err("unexpected payload during concurrent echo jobs".to_owned()),
        }
    }
    Ok(())
}

async fn host_calls_and_log(driver: &mut Driver) -> Result<(), String> {
    let correlation = "00000000-0000-4000-8000-000000000030";
    let job_id = "00000000-0000-4000-8000-000000000031";
    let start_id = start_job(driver, correlation, job_id, "host", &[]).await?;
    expect_job_accepted(driver, correlation, start_id, job_id).await?;

    let get_config = driver.receive(correlation).await?;
    assert_nested_call(&get_config, start_id)?;
    let plugin_envelope::Payload::HostCall(call) = required_payload(get_config.clone())? else {
        return Err("host action did not request configuration".to_owned());
    };
    if !matches!(call.call, Some(oll::host_call_request::Call::GetConfig(_))) {
        return Err("first nested call is not GetConfig".to_owned());
    }
    driver
        .send(
            correlation,
            Some(get_config.message_id),
            plugin_envelope::Payload::HostResult(oll::HostCallResponse {
                result: Some(oll::host_call_response::Result::GetConfig(
                    oll::GetConfigResponse {
                        value: Some(oll::ConfigValue {
                            kind: Some(oll::config_value::Kind::FunctionValue(
                                oll::ConfigFunctionRef {
                                    session_id: SESSION_ID.to_owned(),
                                    function_id: "conformance-function".to_owned(),
                                },
                            )),
                        }),
                    },
                )),
            }),
        )
        .await?;

    let invoke = driver.receive(correlation).await?;
    assert_nested_call(&invoke, start_id)?;
    let plugin_envelope::Payload::HostCall(call) = required_payload(invoke.clone())? else {
        return Err("host action did not invoke the configuration function".to_owned());
    };
    let Some(oll::host_call_request::Call::InvokeConfigFunction(request)) = call.call else {
        return Err("second nested call is not InvokeConfigFunction".to_owned());
    };
    if request
        .function
        .as_ref()
        .map(|value| value.function_id.as_str())
        != Some("conformance-function")
    {
        return Err("plugin changed the configuration function handle".to_owned());
    }
    driver
        .send(
            correlation,
            Some(invoke.message_id),
            plugin_envelope::Payload::HostResult(oll::HostCallResponse {
                result: Some(oll::host_call_response::Result::InvokeConfigFunction(
                    oll::InvokeConfigFunctionResponse {
                        results: vec![string_value("function")],
                    },
                )),
            }),
        )
        .await?;

    let read = driver.receive(correlation).await?;
    assert_nested_call(&read, start_id)?;
    let plugin_envelope::Payload::HostCall(call) = required_payload(read.clone())? else {
        return Err("host action did not read a document".to_owned());
    };
    if !matches!(
        call.call,
        Some(oll::host_call_request::Call::ReadDocument(_))
    ) {
        return Err("third nested call is not ReadDocument".to_owned());
    }
    driver
        .send(
            correlation,
            Some(read.message_id),
            plugin_envelope::Payload::HostResult(oll::HostCallResponse {
                result: Some(oll::host_call_response::Result::ReadDocument(
                    oll::ReadDocumentResponse {
                        document: Some(oll::DocumentSnapshot {
                            metadata: None,
                            representation: Some(oll::document_snapshot::Representation::Content(
                                "document".to_owned(),
                            )),
                        }),
                    },
                )),
            }),
        )
        .await?;

    let log = driver.receive(correlation).await?;
    let plugin_envelope::Payload::Log(log) = required_payload(log)? else {
        return Err("host action did not emit a structured log".to_owned());
    };
    if log.target != "conformance" || log.message != "host action complete" {
        return Err("structured log content differs".to_owned());
    }
    let update = driver.receive(correlation).await?;
    expect_success(update, job_id, "function|document")
}

async fn artifact_transfer(driver: &mut Driver) -> Result<(), String> {
    let correlation = "00000000-0000-4000-8000-000000000040";
    let job_id = "00000000-0000-4000-8000-000000000041";
    let start_id = start_job(driver, correlation, job_id, "artifact", &[]).await?;
    expect_job_accepted(driver, correlation, start_id, job_id).await?;
    let start = driver.receive(correlation).await?;
    let plugin_envelope::Payload::ArtifactStart(transfer) = required_payload(start.clone())? else {
        return Err("artifact action omitted ArtifactTransferStart".to_owned());
    };
    let descriptor = transfer
        .artifact
        .ok_or_else(|| "ArtifactTransferStart omitted descriptor".to_owned())?;
    let artifact_id = descriptor
        .artifact_id
        .as_ref()
        .ok_or_else(|| "artifact descriptor omitted ID".to_owned())?;
    Uuid::parse_str(&artifact_id.value).map_err(|_| "artifact ID is not a UUID".to_owned())?;
    if descriptor.file_name != "conformance.txt"
        || descriptor.media_type != "text/plain"
        || descriptor.size_bytes != 16
        || descriptor.sha256 != Sha256::digest(b"artifact payload").as_slice()
        || transfer.chunk_count != 2
    {
        return Err("artifact descriptor does not match the fixture payload".to_owned());
    }
    driver
        .send(
            correlation,
            Some(start.message_id),
            plugin_envelope::Payload::ArtifactAccepted(oll::ArtifactTransferAccepted {
                artifact_id: Some(artifact_id.clone()),
            }),
        )
        .await?;
    let mut bytes = Vec::new();
    for expected_index in 0..2 {
        let chunk = driver.receive(correlation).await?;
        let payload = required_payload(chunk)?;
        let plugin_envelope::Payload::ArtifactChunk(chunk) = payload else {
            return Err(format!(
                "artifact transfer expected chunk {expected_index}, received {payload:?}"
            ));
        };
        if chunk.chunk_index != expected_index || chunk.artifact_id.as_ref() != Some(artifact_id) {
            return Err("artifact chunk order or identity differs".to_owned());
        }
        bytes.extend_from_slice(&chunk.data);
    }
    if bytes != b"artifact payload" {
        return Err("artifact chunk bytes differ".to_owned());
    }
    let complete = driver.receive(correlation).await?;
    let plugin_envelope::Payload::ArtifactComplete(completed) = required_payload(complete.clone())?
    else {
        return Err("artifact transfer omitted completion".to_owned());
    };
    if completed.artifact_id.as_ref() != Some(artifact_id) {
        return Err("artifact completion changed identity".to_owned());
    }
    driver
        .send(
            correlation,
            Some(complete.message_id),
            plugin_envelope::Payload::ArtifactStored(oll::ArtifactStored {
                artifact_id: Some(artifact_id.clone()),
            }),
        )
        .await?;
    let update = driver.receive(correlation).await?;
    let plugin_envelope::Payload::JobUpdate(update) = required_payload(update)? else {
        return Err("artifact action omitted terminal JobUpdate".to_owned());
    };
    if update.job_id.as_ref().map(|value| value.value.as_str()) != Some(job_id)
        || update.state != oll::JobState::Succeeded as i32
        || update.artifacts != [descriptor]
    {
        return Err("artifact terminal result does not reference stored bytes".to_owned());
    }
    Ok(())
}

async fn shutdown(driver: &mut Driver) -> Result<(), String> {
    let correlation = "00000000-0000-4000-8000-000000000050";
    let deadline = std::time::SystemTime::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| "shutdown deadline overflowed".to_owned())?;
    let deadline = prost_types::Timestamp::try_from(deadline)
        .map_err(|error| format!("create shutdown deadline: {error}"))?;
    let message_id = driver
        .send(
            correlation,
            None,
            plugin_envelope::Payload::Shutdown(oll::ShutdownRequest {
                reason: "conformance complete".to_owned(),
                grace_period_deadline: Some(deadline),
            }),
        )
        .await?;
    let acknowledged = driver.receive(correlation).await?;
    if acknowledged.reply_to != Some(message_id)
        || !matches!(
            required_payload(acknowledged)?,
            plugin_envelope::Payload::ShutdownAcknowledged(_)
        )
    {
        return Err("plugin did not gracefully acknowledge shutdown".to_owned());
    }
    Ok(())
}

async fn start_job(
    driver: &mut Driver,
    correlation_id: &str,
    job_id: &str,
    action: &str,
    arguments: &[&str],
) -> Result<u64, String> {
    driver
        .send(
            correlation_id,
            None,
            plugin_envelope::Payload::StartJob(oll::StartJobRequest {
                job_id: Some(oll::PluginJobId {
                    value: job_id.to_owned(),
                }),
                deadline: None,
                invocation: Some(oll::start_job_request::Invocation::Action(
                    oll::ActionInvocation {
                        action: action.to_owned(),
                        arguments: arguments.iter().map(|value| (*value).to_owned()).collect(),
                    },
                )),
            }),
        )
        .await
}

async fn expect_job_accepted(
    driver: &mut Driver,
    correlation_id: &str,
    start_id: u64,
    job_id: &str,
) -> Result<(), String> {
    let envelope = driver.receive(correlation_id).await?;
    let plugin_envelope::Payload::JobAccepted(accepted) = required_payload(envelope.clone())?
    else {
        return Err("plugin did not send JobAccepted first".to_owned());
    };
    if envelope.reply_to != Some(start_id)
        || accepted.job_id.as_ref().map(|value| value.value.as_str()) != Some(job_id)
    {
        return Err("JobAccepted changed the job or reply target".to_owned());
    }
    Ok(())
}

fn expect_success(
    envelope: oll::PluginEnvelope,
    job_id: &str,
    expected: &str,
) -> Result<(), String> {
    let plugin_envelope::Payload::JobUpdate(update) = required_payload(envelope)? else {
        return Err("action omitted terminal JobUpdate".to_owned());
    };
    if update.job_id.as_ref().map(|value| value.value.as_str()) != Some(job_id)
        || update.state != oll::JobState::Succeeded as i32
        || update.result.as_ref().and_then(config_string) != Some(expected)
    {
        return Err("action returned the wrong terminal result".to_owned());
    }
    Ok(())
}

fn assert_nested_call(envelope: &oll::PluginEnvelope, parent: u64) -> Result<(), String> {
    let trace = envelope
        .trace
        .as_ref()
        .ok_or_else(|| "nested call omitted trace context".to_owned())?;
    if trace.parent_call_id != Some(parent) || trace.call_depth != 1 {
        return Err("nested host call did not set parent_call_id and call_depth".to_owned());
    }
    Ok(())
}

fn validate_unscoped_receive(
    driver: &mut Driver,
    envelope: &oll::PluginEnvelope,
) -> Result<(), String> {
    let encoded_len = envelope.encoded_len();
    if encoded_len > MAXIMUM_ENVELOPE_BYTES
        || envelope.message_id == 0
        || envelope.message_id <= driver.last_plugin_message_id
    {
        return Err(format!(
            "invalid plugin envelope: message_id={}, previous={}, encoded_len={encoded_len}",
            envelope.message_id, driver.last_plugin_message_id
        ));
    }
    driver.last_plugin_message_id = envelope.message_id;
    if envelope.session_id != SESSION_ID
        || envelope.plugin_instance_id != INSTANCE_ID
        || envelope
            .trace
            .as_ref()
            .map(|value| value.correlation_id.is_empty())
            != Some(false)
    {
        return Err("plugin changed session or trace identity".to_owned());
    }
    Ok(())
}

fn required_payload(envelope: oll::PluginEnvelope) -> Result<plugin_envelope::Payload, String> {
    envelope
        .payload
        .ok_or_else(|| "plugin envelope omitted payload".to_owned())
}

fn trace(correlation_id: &str) -> oll::TraceContext {
    oll::TraceContext {
        correlation_id: correlation_id.to_owned(),
        parent_call_id: None,
        call_depth: 0,
        causal_depth: 0,
        task_id: None,
        task_group_id: None,
    }
}

fn string_value(value: &str) -> oll::ConfigValue {
    oll::ConfigValue {
        kind: Some(oll::config_value::Kind::StringValue(value.to_owned())),
    }
}

fn config_string(value: &oll::ConfigValue) -> Option<&str> {
    match value.kind.as_ref()? {
        oll::config_value::Kind::StringValue(value) => Some(value),
        _ => None,
    }
}

use prost::Message as _;
