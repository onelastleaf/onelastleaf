use std::{
    collections::HashSet,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::time::Instant;
use tonic::Streaming;

use crate::{
    node::logging::LogLevel,
    plugin::{InstalledPlugin, PluginInstanceId, protocol},
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{self},
    },
};

use super::{
    super::{InstanceNotice, RuntimeDependencies, supervisor::PluginAction},
    ConnectedSession, SessionOutcome,
    outbound::{root_trace, send_payload, send_protocol_shutdown},
};

pub(super) struct EstablishedSession {
    pub(super) session_id: String,
    pub(super) outgoing_ids: Arc<AtomicU64>,
    pub(super) last_incoming_message_id: u64,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn establish_session(
    dependencies: &RuntimeDependencies,
    plugin: &InstalledPlugin,
    instance_id: PluginInstanceId,
    connected: &mut ConnectedSession,
    notices: &tokio::sync::mpsc::UnboundedSender<InstanceNotice>,
    lifecycle_correlation_id: &str,
    handshake_deadline: Instant,
) -> Result<EstablishedSession, SessionOutcome> {
    let session_id = uuid::Uuid::new_v4().to_string();
    let outgoing_ids = Arc::new(AtomicU64::new(1));
    let mut last_incoming_message_id = 0;
    let hello_trace = root_trace(lifecycle_correlation_id);
    let host_hello = oll::HostHello {
        node: Some(dependencies.identities.node().await.to_proto()),
        session_id: session_id.clone(),
        plugin_instance_id: instance_id.to_string(),
        protocol_schema_sha256: PROTOCOL_SCHEMA_SHA256.to_vec(),
        maximum_call_depth: super::super::MAXIMUM_CALL_DEPTH,
        maximum_causal_depth: super::super::MAXIMUM_CAUSAL_DEPTH,
        maximum_artifact_chunk_bytes: u64::try_from(dependencies.artifacts.maximum_chunk_bytes())
            .unwrap_or(u64::MAX),
        plugin_id: Some(protocol::encode_plugin_id(&plugin.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(&plugin.plugin_name)),
    };
    if send_payload(
        &connected.outgoing,
        &outgoing_ids,
        &session_id,
        instance_id,
        hello_trace.clone(),
        None,
        oll::plugin_envelope::Payload::HostHello(host_hello),
    )
    .await
    .is_err()
    {
        return Err(SessionOutcome::failed(
            "plugin_host_hello_send_failed".to_owned(),
            lifecycle_correlation_id.to_owned(),
        ));
    }

    let handshake = async {
        let (envelope, trace) = receive_envelope(
            &mut connected.incoming,
            &session_id,
            instance_id,
            &mut last_incoming_message_id,
        )
        .await?;
        validate_handshake_trace(&trace, &hello_trace)?;
        if envelope.reply_to.is_some() {
            return Err("PluginHello must not reply to another message".to_owned());
        }
        let Some(oll::plugin_envelope::Payload::PluginHello(hello)) = envelope.payload else {
            return Err("PluginHello must be the first plugin message".to_owned());
        };
        let actions = validate_plugin_hello(plugin, &hello)?;
        send_payload(
            &connected.outgoing,
            &outgoing_ids,
            &session_id,
            instance_id,
            hello_trace.clone(),
            None,
            oll::plugin_envelope::Payload::Ready(oll::SessionReady {}),
        )
        .await
        .map_err(|_| "cannot send host SessionReady".to_owned())?;
        let (envelope, trace) = receive_envelope(
            &mut connected.incoming,
            &session_id,
            instance_id,
            &mut last_incoming_message_id,
        )
        .await?;
        validate_handshake_trace(&trace, &hello_trace)?;
        if envelope.reply_to.is_some()
            || !matches!(
                envelope.payload,
                Some(oll::plugin_envelope::Payload::Ready(_))
            )
        {
            return Err("plugin SessionReady must follow PluginHello".to_owned());
        }
        Ok::<_, String>(actions)
    };
    let actions = match tokio::time::timeout_at(handshake_deadline, handshake).await {
        Ok(Ok(actions)) => actions,
        Ok(Err(error)) => {
            send_protocol_shutdown(
                &connected.outgoing,
                &outgoing_ids,
                &session_id,
                instance_id,
                lifecycle_correlation_id,
                &error,
            )
            .await;
            return Err(SessionOutcome::failed_after_shutdown(
                "plugin_handshake_invalid".to_owned(),
                lifecycle_correlation_id.to_owned(),
            ));
        }
        Err(_) => {
            send_protocol_shutdown(
                &connected.outgoing,
                &outgoing_ids,
                &session_id,
                instance_id,
                lifecycle_correlation_id,
                "plugin handshake deadline exceeded",
            )
            .await;
            return Err(SessionOutcome::failed_after_shutdown(
                "plugin_handshake_timeout".to_owned(),
                lifecycle_correlation_id.to_owned(),
            ));
        }
    };

    let config = dependencies.config.clone();
    let begin_session_id = session_id.clone();
    let begin_plugin_id = plugin.plugin_id.to_string();
    let mut begin_config = tokio::task::spawn_blocking(move || {
        config.begin_plugin_session(&begin_session_id, &begin_plugin_id)
    });
    match tokio::time::timeout_at(handshake_deadline, &mut begin_config).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            send_protocol_shutdown(
                &connected.outgoing,
                &outgoing_ids,
                &session_id,
                instance_id,
                lifecycle_correlation_id,
                &error.to_string(),
            )
            .await;
            return Err(SessionOutcome::failed_after_shutdown(
                "plugin_config_session_failed".to_owned(),
                lifecycle_correlation_id.to_owned(),
            ));
        }
        Ok(Err(_)) => {
            send_protocol_shutdown(
                &connected.outgoing,
                &outgoing_ids,
                &session_id,
                instance_id,
                lifecycle_correlation_id,
                "plugin configuration task failed",
            )
            .await;
            return Err(SessionOutcome::failed_after_shutdown(
                "plugin_config_session_failed".to_owned(),
                lifecycle_correlation_id.to_owned(),
            ));
        }
        Err(_) => {
            let config = dependencies.config.clone();
            let abandoned_session_id = session_id.clone();
            let logger = dependencies.logger.clone();
            let plugin_id = plugin.plugin_id.clone();
            let cleanup_correlation_id = lifecycle_correlation_id.to_owned();
            tokio::spawn(async move {
                if matches!(begin_config.await, Ok(Ok(()))) {
                    let cleanup = tokio::task::spawn_blocking(move || {
                        config.end_plugin_session(&abandoned_session_id)
                    })
                    .await;
                    if !matches!(cleanup, Ok(Ok(()))) {
                        logger.emit(
                            LogLevel::Warn,
                            "oll::plugin",
                            "plugin_config_session_cleanup_failed",
                            &cleanup_correlation_id,
                            serde_json::json!({ "plugin_id": plugin_id.as_str() }),
                        );
                    }
                }
            });
            send_protocol_shutdown(
                &connected.outgoing,
                &outgoing_ids,
                &session_id,
                instance_id,
                lifecycle_correlation_id,
                "plugin configuration startup deadline exceeded",
            )
            .await;
            return Err(SessionOutcome::failed_after_shutdown(
                "plugin_config_session_timeout".to_owned(),
                lifecycle_correlation_id.to_owned(),
            ));
        }
    }
    let _ = notices.send(InstanceNotice::Ready {
        plugin_id: plugin.plugin_id.clone(),
        instance_id,
        actions,
        correlation_id: lifecycle_correlation_id.to_owned(),
    });

    Ok(EstablishedSession {
        session_id,
        outgoing_ids,
        last_incoming_message_id,
    })
}

pub(super) fn validate_plugin_hello(
    plugin: &InstalledPlugin,
    hello: &oll::PluginHello,
) -> Result<Vec<PluginAction>, String> {
    if protocol::decode_plugin_id(hello.plugin_id.as_ref(), "PluginHello.plugin_id")
        .map_err(|error| error.to_string())?
        != plugin.plugin_id
        || protocol::decode_plugin_name(hello.plugin_name.as_ref(), "PluginHello.plugin_name")
            .map_err(|error| error.to_string())?
            != plugin.plugin_name
    {
        return Err("PluginHello identity differs from the spawned package".to_owned());
    }
    if hello.protocol_schema_sha256.as_slice() != PROTOCOL_SCHEMA_SHA256 {
        return Err("PluginHello protocol fingerprint differs".to_owned());
    }
    let mut names = HashSet::new();
    let mut actions = Vec::with_capacity(hello.actions.len());
    for action in &hello.actions {
        if action.name.is_empty() || !names.insert(action.name.clone()) {
            return Err("PluginHello action names must be nonempty and unique".to_owned());
        }
        actions.push(PluginAction {
            name: action.name.clone(),
            description: action.description.clone(),
        });
    }
    Ok(actions)
}

pub(in crate::plugin::runtime) fn validate_handshake_trace(
    trace: &oll::TraceContext,
    lifecycle_root: &oll::TraceContext,
) -> Result<(), String> {
    if trace != lifecycle_root {
        return Err(
            "plugin handshake trace must exactly inherit the HostHello lifecycle root".to_owned(),
        );
    }
    Ok(())
}

pub(super) async fn receive_envelope(
    incoming: &mut Streaming<oll::PluginEnvelope>,
    session_id: &str,
    instance_id: PluginInstanceId,
    last_seen_message_id: &mut u64,
) -> Result<(oll::PluginEnvelope, oll::TraceContext), String> {
    let envelope = incoming
        .message()
        .await
        .map_err(|_| "plugin stream failed".to_owned())?
        .ok_or_else(|| "plugin stream closed".to_owned())?;
    let trace = validate_envelope(&envelope, session_id, instance_id, last_seen_message_id)?;
    Ok((envelope, trace))
}

pub(in crate::plugin::runtime) fn validate_envelope(
    envelope: &oll::PluginEnvelope,
    session_id: &str,
    instance_id: PluginInstanceId,
    last_seen_message_id: &mut u64,
) -> Result<oll::TraceContext, String> {
    if envelope.message_id == 0 || envelope.message_id <= *last_seen_message_id {
        return Err("plugin message_id must be nonzero and strictly increasing".to_owned());
    }
    if envelope.session_id != session_id || envelope.plugin_instance_id != instance_id.to_string() {
        return Err("plugin envelope belongs to another session or instance".to_owned());
    }
    let trace = envelope
        .trace
        .clone()
        .ok_or_else(|| "plugin envelope trace context is required".to_owned())?;
    if trace.correlation_id.is_empty() {
        return Err("plugin envelope correlation_id must not be empty".to_owned());
    }
    if trace.call_depth > super::super::MAXIMUM_CALL_DEPTH {
        return Err("plugin call depth exceeds the negotiated limit".to_owned());
    }
    if trace.causal_depth > super::super::MAXIMUM_CAUSAL_DEPTH {
        return Err("plugin causal depth exceeds the negotiated limit".to_owned());
    }
    *last_seen_message_id = envelope.message_id;
    Ok(trace)
}
