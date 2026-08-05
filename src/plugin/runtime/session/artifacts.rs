use std::sync::{Arc, atomic::AtomicU64};

use time::OffsetDateTime;
use tokio::sync::mpsc;
use tonic::Status;

use crate::{
    plugin::{PluginId, PluginInstanceId},
    protocol::oll::{self, plugin_envelope},
};

use super::{
    super::{RuntimeDependencies, host::plugin_error},
    outbound::send_payload,
};

pub(super) enum ArtifactCommand {
    Start {
        message_id: u64,
        trace: oll::TraceContext,
        request: oll::ArtifactTransferStart,
    },
    Chunk {
        message_id: u64,
        trace: oll::TraceContext,
        request: oll::ArtifactTransferChunk,
    },
    Complete {
        message_id: u64,
        trace: oll::TraceContext,
        request: oll::ArtifactTransferComplete,
    },
}

pub(super) async fn run_artifacts(
    dependencies: RuntimeDependencies,
    plugin_id: PluginId,
    instance_id: PluginInstanceId,
    session_id: String,
    outgoing: mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    outgoing_ids: Arc<AtomicU64>,
    mut receiver: mpsc::Receiver<ArtifactCommand>,
) {
    let mut artifacts = dependencies.artifacts.session(plugin_id, instance_id);
    while let Some(command) = receiver.recv().await {
        let (message_id, trace, result) = match command {
            ArtifactCommand::Start {
                message_id,
                trace,
                request,
            } => (
                message_id,
                trace.clone(),
                artifacts
                    .start_transfer(&request, &trace.correlation_id)
                    .await
                    .map(|reply| Some(plugin_envelope::Payload::ArtifactAccepted(reply))),
            ),
            ArtifactCommand::Chunk {
                message_id,
                trace,
                request,
            } => (
                message_id,
                trace.clone(),
                artifacts
                    .receive_chunk(&request, &trace.correlation_id)
                    .await
                    .map(|()| None),
            ),
            ArtifactCommand::Complete {
                message_id,
                trace,
                request,
            } => (
                message_id,
                trace.clone(),
                artifacts
                    .complete_transfer(&request, &trace.correlation_id, OffsetDateTime::now_utc())
                    .await
                    .map(|(reply, _)| Some(plugin_envelope::Payload::ArtifactStored(reply))),
            ),
        };
        match result {
            Ok(Some(reply)) => {
                let _ = send_payload(
                    &outgoing,
                    &outgoing_ids,
                    &session_id,
                    instance_id,
                    trace,
                    Some(message_id),
                    reply,
                )
                .await;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = send_payload(
                    &outgoing,
                    &outgoing_ids,
                    &session_id,
                    instance_id,
                    trace,
                    Some(message_id),
                    plugin_envelope::Payload::ProtocolError(plugin_error(error)),
                )
                .await;
            }
        }
    }
    let _ = artifacts
        .abort_all("plugin_session_ended", OffsetDateTime::now_utc())
        .await;
}
