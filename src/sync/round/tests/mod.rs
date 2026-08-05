use std::{fs, time::Duration};

use loro::{ExportMode, LoroDoc, UpdateOptions};
use tempfile::TempDir;
use tokio::io::duplex;

use crate::{
    configuration::{NetworkKey, ReplicaStoreConfig},
    node::{NodeIdentity, identity::IdentityCoordinator, logging::NodeLogger},
    protocol::oll::{
        BlobTransferChunk, BlobTransferComplete, BlobTransferStart, ReplicaTransferChunk,
        ReplicaTransferComplete, ReplicaTransferStart,
    },
    sync::{HANDSHAKE_DEADLINE, NoiseTransport, derive_noise_psk},
};

use super::*;

async fn test_channels() -> (
    SessionChannel<tokio::io::DuplexStream>,
    SessionChannel<tokio::io::DuplexStream>,
) {
    let key = derive_noise_psk(&NetworkKey::new_for_test(vec![42; 32]));
    let (initiator_stream, responder_stream) = duplex(16 * 1024);
    let deadline = tokio::time::Instant::now() + HANDSHAKE_DEADLINE;
    let (initiator, responder) = tokio::join!(
        NoiseTransport::connect(initiator_stream, &key, deadline),
        NoiseTransport::accept(responder_stream, &key, deadline),
    );
    (
        SessionChannel::new(initiator.unwrap()),
        SessionChannel::new(responder.unwrap()),
    )
}

mod chunks;
mod liveness;
mod protocol;
mod rejection;
