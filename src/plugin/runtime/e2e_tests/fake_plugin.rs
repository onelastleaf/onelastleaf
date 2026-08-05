use std::{collections::HashMap, io::Read as _, time::Duration};

use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Streaming, transport::Endpoint};

use crate::{
    plugin::protocol,
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{self, plugin_envelope, plugin_runtime_client::PluginRuntimeClient},
    },
};

pub(super) const FAKE_PLUGIN_TEST: &str =
    "plugin::runtime::e2e_tests::fake_plugin::fake_plugin_process";

const CHILD_DEADLINE: Duration = Duration::from_secs(20);
const PLUGIN_ID: &str = "oll.runtime-e2e";
const PLUGIN_NAME: &str = "runtime-e2e";
const DOCUMENT_PATH: &str = "/runtime-e2e.md";
const LARGE_DOCUMENT_PATH: &str = "/runtime-e2e-large.md";
const LARGE_DOCUMENT_BYTES: usize = 5_000_000;

/// Subprocess entry point used by the runtime E2E test. The ordinary test run
/// leaves it ignored; the installed test package invokes this exact libtest
/// case in a child process and supplies the production endpoint environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess-only fake plugin entry point"]
async fn fake_plugin_process() {
    let Some(endpoint) = std::env::var_os("OLL_PLUGIN_ENDPOINT") else {
        // Keep an explicit `cargo test -- --ignored` harmless. Only the child
        // spawned by the plugin supervisor owns an instance endpoint.
        return;
    };
    let endpoint = endpoint
        .into_string()
        .expect("OLL_PLUGIN_ENDPOINT is valid UTF-8");
    tokio::time::timeout(CHILD_DEADLINE, run_fake_plugin(endpoint))
        .await
        .expect("fake plugin exceeded its absolute deadline")
        .expect("fake plugin protocol session failed");
}

async fn run_fake_plugin(endpoint: String) -> Result<(), String> {
    let channel = Endpoint::from_shared(endpoint)
        .map_err(|error| error.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    let (outgoing, receiver) = mpsc::channel(64);
    let mut client = PluginRuntimeClient::new(channel)
        .max_decoding_message_size(super::super::MAX_PLUGIN_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(super::super::MAX_PLUGIN_GRPC_MESSAGE_BYTES);
    let incoming = client
        .connect(Request::new(ReceiverStream::new(receiver)))
        .await
        .map_err(|error| error.to_string())?
        .into_inner();
    let (stdin_eof, stdin_eof_rx) = oneshot::channel();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buffer = [0_u8; 1];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        let _ = stdin_eof.send(());
    });

    let mut session = FakeSession {
        incoming,
        outgoing,
        stdin_eof: stdin_eof_rx,
        session_id: String::new(),
        instance_id: String::new(),
        next_message_id: 1,
        last_host_message_id: 0,
    };
    session.handshake().await?;
    session.update_document().await?;
    session
        .emit_log("runtime-e2e-host-calls", "host calls completed")
        .await?;
    if std::path::Path::new("no-read-flood").exists() {
        session.flood_host_calls_without_reading().await?;
        return std::future::pending().await;
    }
    let exit_once = std::path::Path::new("exit-once");
    if exit_once.exists() {
        std::fs::remove_file(exit_once).map_err(|error| error.to_string())?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::process::exit(23);
    }
    session.run().await
}

struct FakeSession {
    incoming: Streaming<oll::PluginEnvelope>,
    outgoing: mpsc::Sender<oll::PluginEnvelope>,
    stdin_eof: oneshot::Receiver<()>,
    session_id: String,
    instance_id: String,
    next_message_id: u64,
    last_host_message_id: u64,
}

impl FakeSession {
    async fn handshake(&mut self) -> Result<(), String> {
        let envelope = self.receive().await?;
        if envelope.reply_to.is_some() {
            return Err("HostHello unexpectedly replies to another message".to_owned());
        }
        let trace = required_trace(&envelope)?.clone();
        let Some(plugin_envelope::Payload::HostHello(hello)) = envelope.payload else {
            return Err("HostHello was not the first host message".to_owned());
        };
        if hello.node.is_none()
            || hello.session_id.is_empty()
            || hello.plugin_instance_id.is_empty()
            || hello.protocol_schema_sha256.as_slice() != PROTOCOL_SCHEMA_SHA256
            || hello.plugin_id.as_ref().map(|value| value.value.as_str()) != Some(PLUGIN_ID)
            || hello.plugin_name.as_ref().map(|value| value.value.as_str()) != Some(PLUGIN_NAME)
            || hello.maximum_call_depth != super::super::MAXIMUM_CALL_DEPTH
            || hello.maximum_causal_depth != super::super::MAXIMUM_CAUSAL_DEPTH
            || hello.maximum_artifact_chunk_bytes == 0
        {
            return Err("HostHello does not describe the expected instance".to_owned());
        }
        self.session_id = hello.session_id;
        self.instance_id = hello.plugin_instance_id;

        self.send(
            None,
            trace.clone(),
            plugin_envelope::Payload::PluginHello(oll::PluginHello {
                plugin_id: Some(oll::PluginId {
                    value: PLUGIN_ID.to_owned(),
                }),
                plugin_name: Some(oll::PluginName {
                    value: PLUGIN_NAME.to_owned(),
                }),
                protocol_schema_sha256: PROTOCOL_SCHEMA_SHA256.to_vec(),
                actions: vec![oll::ActionDescriptor {
                    name: "hold".to_owned(),
                    description: "Hold a test job until cancellation or shutdown".to_owned(),
                }],
                plugin_version: "runtime-e2e-test".to_owned(),
            }),
        )
        .await?;

        let ready = self.receive().await?;
        if ready.reply_to.is_some()
            || !matches!(ready.payload, Some(plugin_envelope::Payload::Ready(_)))
        {
            return Err("host SessionReady did not follow PluginHello".to_owned());
        }
        self.send(
            None,
            trace,
            plugin_envelope::Payload::Ready(oll::SessionReady {}),
        )
        .await?;
        Ok(())
    }

    async fn update_document(&mut self) -> Result<(), String> {
        let tree = self
            .host_call(
                "runtime-e2e-directory-tree",
                oll::host_call_request::Call::GetDirectoryTree(oll::GetDirectoryTreeRequest {
                    root: Some(oll::DocumentPath {
                        value: "/".to_owned(),
                    }),
                }),
            )
            .await?;
        let Some(oll::host_call_response::Result::GetDirectoryTree(tree)) = tree.result else {
            return Err("GetDirectoryTree host call did not return a tree".to_owned());
        };
        if !tree.root.as_ref().is_some_and(|root| {
            tree_contains_path(root, DOCUMENT_PATH) && tree_contains_path(root, LARGE_DOCUMENT_PATH)
        }) {
            return Err("GetDirectoryTree omitted the E2E document".to_owned());
        }

        let large = self
            .host_call(
                "runtime-e2e-read-document",
                oll::host_call_request::Call::ReadDocument(oll::ReadDocumentRequest {
                    path: Some(oll::DocumentPath {
                        value: LARGE_DOCUMENT_PATH.to_owned(),
                    }),
                    projection: oll::DocumentProjection::Content as i32,
                }),
            )
            .await?;
        let Some(oll::host_call_response::Result::ReadDocument(large)) = large.result else {
            return Err("ReadDocument host call did not return a document".to_owned());
        };
        let large = large
            .document
            .ok_or_else(|| "ReadDocument response omitted the snapshot".to_owned())?;
        let large_content = match large.representation.as_ref() {
            Some(oll::document_snapshot::Representation::Content(content)) => content,
            _ => return Err("ReadDocument omitted the document body".to_owned()),
        };
        if large_content.len() != LARGE_DOCUMENT_BYTES {
            return Err("ReadDocument truncated the large document body".to_owned());
        }

        let read = self
            .host_call(
                "runtime-e2e-read-mutable-document",
                oll::host_call_request::Call::ReadDocument(oll::ReadDocumentRequest {
                    path: Some(oll::DocumentPath {
                        value: DOCUMENT_PATH.to_owned(),
                    }),
                    projection: oll::DocumentProjection::Content as i32,
                }),
            )
            .await?;
        let Some(oll::host_call_response::Result::ReadDocument(read)) = read.result else {
            return Err("mutable ReadDocument host call did not return a document".to_owned());
        };
        let snapshot = read
            .document
            .ok_or_else(|| "mutable ReadDocument response omitted the snapshot".to_owned())?;
        let content = match snapshot.representation.as_ref() {
            Some(oll::document_snapshot::Representation::Content(content)) => content,
            _ => return Err("mutable ReadDocument omitted the document body".to_owned()),
        };
        if content == "updated by fake plugin" {
            return Ok(());
        }
        let metadata = snapshot
            .metadata
            .ok_or_else(|| "ReadDocument response omitted metadata".to_owned())?;
        let document_id = metadata
            .document_id
            .ok_or_else(|| "ReadDocument metadata omitted DocumentId".to_owned())?;
        let revision = metadata
            .document_revision
            .ok_or_else(|| "ReadDocument metadata omitted DocumentRevision".to_owned())?;

        let commit = self
            .host_call(
                "runtime-e2e-commit-document",
                oll::host_call_request::Call::CommitDocuments(oll::CommitDocumentsRequest {
                    operation_id: "runtime-e2e-plugin-commit".to_owned(),
                    preconditions: vec![oll::CommitPrecondition {
                        condition: Some(oll::commit_precondition::Condition::DocumentUnchanged(
                            oll::DocumentRevisionPrecondition {
                                document_id: Some(document_id),
                                unchanged_since: Some(revision),
                            },
                        )),
                    }],
                    mutations: vec![oll::DocumentMutation {
                        mutation: Some(oll::document_mutation::Mutation::ReplaceDocument(
                            oll::ReplaceDocument {
                                path: Some(oll::DocumentPath {
                                    value: DOCUMENT_PATH.to_owned(),
                                }),
                                content: "updated by fake plugin".to_owned(),
                                media_type: None,
                            },
                        )),
                    }],
                }),
            )
            .await?;
        if !matches!(
            commit.result,
            Some(oll::host_call_response::Result::CommitDocuments(_))
        ) {
            return Err("CommitDocuments host call failed".to_owned());
        }
        Ok(())
    }

    async fn host_call(
        &mut self,
        correlation_id: &str,
        call: oll::host_call_request::Call,
    ) -> Result<oll::HostCallResponse, String> {
        let request_id = self
            .send(
                None,
                rich_trace(correlation_id),
                plugin_envelope::Payload::HostCall(oll::HostCallRequest { call: Some(call) }),
            )
            .await?;
        loop {
            let envelope = self.receive().await?;
            if envelope.reply_to == Some(request_id) {
                return match envelope.payload {
                    Some(plugin_envelope::Payload::HostResult(response)) => Ok(response),
                    Some(plugin_envelope::Payload::ProtocolError(error)) => Err(format!(
                        "host rejected a fake plugin call: {}",
                        error.message
                    )),
                    _ => Err("host call received an unexpected direct response".to_owned()),
                };
            }
            self.handle_control_while_host_call_pending(envelope)
                .await?;
        }
    }

    async fn flood_host_calls_without_reading(&mut self) -> Result<(), String> {
        for index in 0..512_u64 {
            self.send(
                None,
                trace(&format!("runtime-e2e-flood-{index}")),
                plugin_envelope::Payload::HostCall(oll::HostCallRequest {
                    call: Some(oll::host_call_request::Call::GetDirectoryTree(
                        oll::GetDirectoryTreeRequest {
                            root: Some(oll::DocumentPath {
                                value: "/".to_owned(),
                            }),
                        },
                    )),
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<(), String> {
        let mut active_jobs = HashMap::new();
        loop {
            let envelope = self.receive().await?;
            let reply_to = envelope.message_id;
            let trace = required_trace(&envelope)?.clone();
            match envelope.payload {
                Some(plugin_envelope::Payload::StartJob(request)) => {
                    let job_id = request
                        .job_id
                        .ok_or_else(|| "StartJobRequest omitted JobId".to_owned())?;
                    let Some(oll::start_job_request::Invocation::Action(action)) =
                        request.invocation
                    else {
                        return Err("fake plugin received a non-action job".to_owned());
                    };
                    let expected_cancellation = if action
                        .arguments
                        .iter()
                        .any(|value| value == "expect-deadline-cancel")
                    {
                        oll::JobCancellationReason::Deadline
                    } else {
                        oll::JobCancellationReason::UserRequest
                    };
                    if action.action != "hold"
                        || active_jobs
                            .insert(job_id.value.clone(), expected_cancellation)
                            .is_some()
                    {
                        return Err("fake plugin received an invalid or duplicate job".to_owned());
                    }
                    let complete_immediately = action
                        .arguments
                        .iter()
                        .any(|value| value == "complete-immediately");
                    if action.arguments.iter().any(|value| value == "delay-accept") {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    self.send(
                        Some(reply_to),
                        trace.clone(),
                        plugin_envelope::Payload::JobAccepted(oll::JobAccepted {
                            job_id: Some(job_id.clone()),
                        }),
                    )
                    .await?;
                    if complete_immediately {
                        active_jobs.remove(&job_id.value);
                        self.send(
                            None,
                            trace,
                            plugin_envelope::Payload::JobUpdate(oll::JobUpdate {
                                job_id: Some(job_id),
                                state: oll::JobState::Succeeded as i32,
                                progress: Some(1.0),
                                status_message: None,
                                result: None,
                                error: None,
                                artifacts: Vec::new(),
                            }),
                        )
                        .await?;
                    }
                }
                Some(plugin_envelope::Payload::CancelJob(request)) => {
                    let job_id = request
                        .job_id
                        .ok_or_else(|| "CancelJobRequest omitted JobId".to_owned())?;
                    let cancellation = oll::JobCancellationReason::try_from(request.reason).ok();
                    let expected = active_jobs.remove(&job_id.value);
                    if expected.is_none() || expected != cancellation {
                        return Err("fake plugin received an invalid job cancellation".to_owned());
                    }
                    self.send(
                        Some(reply_to),
                        trace,
                        plugin_envelope::Payload::CancelJobAcknowledged(
                            oll::CancelJobAcknowledged {
                                job_id: Some(job_id),
                            },
                        ),
                    )
                    .await?;
                }
                Some(plugin_envelope::Payload::Heartbeat(heartbeat)) => {
                    self.send(
                        Some(reply_to),
                        trace,
                        plugin_envelope::Payload::Heartbeat(heartbeat),
                    )
                    .await?;
                }
                Some(plugin_envelope::Payload::Shutdown(request)) => {
                    if request.reason.is_empty() || request.grace_period_deadline.is_none() {
                        return Err("ShutdownRequest is incomplete".to_owned());
                    }
                    if active_jobs.len() > 1 {
                        return Err(format!(
                            "shutdown preserved {} active jobs after job cancellation",
                            active_jobs.len(),
                        ));
                    }
                    self.emit_log(&trace.correlation_id, "shutdown request observed")
                        .await?;
                    self.send(
                        Some(reply_to),
                        trace,
                        plugin_envelope::Payload::ShutdownAcknowledged(
                            oll::ShutdownAcknowledged {},
                        ),
                    )
                    .await?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return Ok(());
                }
                Some(plugin_envelope::Payload::ProtocolError(error)) => {
                    return Err(format!("host reported a protocol error: {}", error.message));
                }
                _ => return Err("fake plugin received an unexpected host message".to_owned()),
            }
        }
    }

    async fn handle_control_while_host_call_pending(
        &mut self,
        envelope: oll::PluginEnvelope,
    ) -> Result<(), String> {
        let reply_to = envelope.message_id;
        let trace = required_trace(&envelope)?.clone();
        match envelope.payload {
            Some(plugin_envelope::Payload::Heartbeat(heartbeat)) => {
                self.send(
                    Some(reply_to),
                    trace,
                    plugin_envelope::Payload::Heartbeat(heartbeat),
                )
                .await?;
                Ok(())
            }
            Some(plugin_envelope::Payload::Shutdown(request)) => {
                if request.reason.is_empty() || request.grace_period_deadline.is_none() {
                    return Err("ShutdownRequest is incomplete".to_owned());
                }
                self.emit_log(&trace.correlation_id, "shutdown request observed")
                    .await?;
                self.send(
                    Some(reply_to),
                    trace,
                    plugin_envelope::Payload::ShutdownAcknowledged(oll::ShutdownAcknowledged {}),
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                std::process::exit(0);
            }
            _ => Err("unexpected host message while a host call was pending".to_owned()),
        }
    }

    async fn emit_log(&mut self, correlation_id: &str, message: &str) -> Result<(), String> {
        let timestamp = protocol::encode_timestamp(OffsetDateTime::now_utc(), "fake plugin log")
            .map_err(|error| error.to_string())?;
        self.send(
            None,
            rich_trace(correlation_id),
            plugin_envelope::Payload::Log(oll::LogRecord {
                timestamp: Some(timestamp),
                level: oll::LogLevel::Info as i32,
                target: "plugin::runtime_e2e".to_owned(),
                message: message.to_owned(),
                fields: std::collections::HashMap::from([
                    (
                        "parent_call_id".to_owned(),
                        oll::ConfigValue {
                            kind: Some(oll::config_value::Kind::IntegerValue(9999)),
                        },
                    ),
                    (
                        "task_id".to_owned(),
                        oll::ConfigValue {
                            kind: Some(oll::config_value::Kind::StringValue(
                                "plugin-spoofed-task".to_owned(),
                            )),
                        },
                    ),
                ]),
            }),
        )
        .await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<oll::PluginEnvelope, String> {
        let envelope = tokio::select! {
            _ = &mut self.stdin_eof => return Err("parent-liveness stdin reached EOF".to_owned()),
            incoming = self.incoming.message() => incoming
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "host closed the plugin stream".to_owned())?,
        };
        if envelope.message_id == 0 || envelope.message_id <= self.last_host_message_id {
            return Err("host message IDs must be nonzero and strictly increasing".to_owned());
        }
        if !self.session_id.is_empty()
            && (envelope.session_id != self.session_id
                || envelope.plugin_instance_id != self.instance_id)
        {
            return Err("host envelope belongs to another plugin instance".to_owned());
        }
        required_trace(&envelope)?;
        self.last_host_message_id = envelope.message_id;
        Ok(envelope)
    }

    async fn send(
        &mut self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<u64, String> {
        let message_id = self.next_message_id;
        self.next_message_id = self
            .next_message_id
            .checked_add(1)
            .ok_or_else(|| "fake plugin exhausted message IDs".to_owned())?;
        self.outgoing
            .send(oll::PluginEnvelope {
                message_id,
                reply_to,
                session_id: self.session_id.clone(),
                plugin_instance_id: self.instance_id.clone(),
                trace: Some(trace),
                payload: Some(payload),
            })
            .await
            .map_err(|_| "fake plugin output stream closed".to_owned())?;
        Ok(message_id)
    }
}

fn tree_contains_path(node: &oll::DirectoryTreeNode, path: &str) -> bool {
    node.metadata
        .as_ref()
        .and_then(|metadata| metadata.path.as_ref())
        .is_some_and(|value| value.value == path)
        || node
            .children
            .iter()
            .any(|child| tree_contains_path(child, path))
}

fn required_trace(envelope: &oll::PluginEnvelope) -> Result<&oll::TraceContext, String> {
    envelope
        .trace
        .as_ref()
        .filter(|trace| !trace.correlation_id.is_empty())
        .ok_or_else(|| "host envelope omitted its correlation context".to_owned())
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

fn rich_trace(correlation_id: &str) -> oll::TraceContext {
    oll::TraceContext {
        parent_call_id: Some(4242),
        call_depth: 2,
        causal_depth: 3,
        task_id: Some("runtime-e2e-task".to_owned()),
        task_group_id: Some("runtime-e2e-group".to_owned()),
        ..trace(correlation_id)
    }
}
