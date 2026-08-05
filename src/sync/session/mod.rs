use std::fmt;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    node::identity::NodeIdentity,
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{
            NoLocalReplica, ReplicaId, SyncClose, SyncCloseCode, SyncEnvelope, SyncHello,
            SyncReady, sync_envelope, sync_hello,
        },
    },
    replica::ReplicaStatus,
};

use super::transport::{MAX_CHUNK_BYTES, NoiseTransport, TransportError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionReplicaMode {
    Waiting,
    Normal,
    BootstrapSource,
    BootstrapReceiver,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionError {
    Transport(TransportError),
    LocalProtocol {
        code: SyncCloseCode,
        error_code: &'static str,
        message: &'static str,
    },
    RemoteClosed {
        code: SyncCloseCode,
        message: String,
    },
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::LocalProtocol { message, .. } => formatter.write_str(message),
            Self::RemoteClosed { message, .. } if !message.is_empty() => {
                formatter.write_str(message)
            }
            Self::RemoteClosed { .. } => formatter.write_str("remote sync session closed"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<TransportError> for SessionError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

mod channel;
mod handshake;

#[cfg(test)]
mod tests;

pub(crate) use channel::SessionChannel;
pub(crate) use handshake::PendingSession;
