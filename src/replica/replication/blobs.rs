use std::collections::BTreeMap;

use super::{
    super::{
        ReplicaError,
        store::{NewBlob, ReplicaStore},
        types::{ActiveReplica, CatalogEntry},
    },
    types::StagedBlob,
};

pub(super) async fn validate_candidate_blobs(
    store: &ReplicaStore,
    candidate: &ActiveReplica,
    received: &BTreeMap<String, StagedBlob>,
    local: &[NewBlob],
) -> Result<(), ReplicaError> {
    let references = candidate
        .entries
        .values()
        .filter_map(CatalogEntry::binary)
        .flat_map(|binary| binary.versions.values())
        .map(|version| (version.sha256.as_str(), version.size_bytes))
        .collect::<BTreeMap<_, _>>();
    if received
        .keys()
        .any(|sha256| !references.contains_key(sha256.as_str()))
        || local
            .iter()
            .any(|blob| !references.contains_key(blob.sha256.as_str()))
    {
        return Err(ReplicaError::InvalidArgument(
            "received blob is not referenced by the merged catalog".to_owned(),
        ));
    }
    for (sha256, expected_size) in references {
        if let Some(blob) = received.get(sha256) {
            if blob.size_bytes != expected_size {
                return Err(ReplicaError::InvalidArgument(
                    "received blob hash or size differs from catalog metadata".to_owned(),
                ));
            }
        } else if let Some(blob) = local.iter().find(|blob| blob.sha256 == sha256) {
            if blob.size_bytes()? != expected_size {
                return Err(ReplicaError::InvalidArgument(
                    "local bootstrap blob size differs from catalog metadata".to_owned(),
                ));
            }
        } else if store.blob_size(sha256).await? != expected_size {
            return Err(ReplicaError::CorruptStore(
                "retained blob size differs from catalog metadata".to_owned(),
            ));
        }
    }
    Ok(())
}
