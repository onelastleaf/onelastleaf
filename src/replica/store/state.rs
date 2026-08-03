use sha2::{Digest, Sha256};

use super::super::types::ActiveReplica;

pub(super) fn state_token(replica: &ActiveReplica) -> [u8; 32] {
    fn field(hash: &mut Sha256, bytes: &[u8]) {
        hash.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        hash.update(bytes);
    }

    let mut hash = Sha256::new();
    field(&mut hash, replica.root_catalog_node_id.as_bytes());
    field(&mut hash, &replica.loro_peer_id.to_be_bytes());
    field(&mut hash, &replica.lamport_clock.to_be_bytes());
    field(&mut hash, &replica.projection_generation.to_be_bytes());
    field(&mut hash, &replica.catalog_loro);
    for (document_id, document) in &replica.documents {
        field(&mut hash, document_id.as_bytes());
        field(&mut hash, &document.loro);
    }
    hash.finalize().into()
}
