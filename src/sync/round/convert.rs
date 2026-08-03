use super::*;

pub(super) fn replica_object_to_proto(object: ReplicaObject) -> ReplicaObjectRef {
    ReplicaObjectRef {
        object: Some(match object {
            ReplicaObject::Catalog => replica_object_ref::Object::Catalog(CatalogObject {}),
            ReplicaObject::Document(document_id) => {
                replica_object_ref::Object::Document(crate::protocol::oll::DocumentId {
                    value: document_id.to_string(),
                })
            }
        }),
    }
}

pub(super) fn replica_object_from_proto(
    object: &ReplicaObjectRef,
) -> Result<ReplicaObject, RoundError> {
    match object.object.as_ref() {
        Some(replica_object_ref::Object::Catalog(_)) => Ok(ReplicaObject::Catalog),
        Some(replica_object_ref::Object::Document(document)) => {
            let id = Uuid::parse_str(&document.value)
                .map_err(|_| RoundError::Protocol("DocumentId is invalid"))?;
            if id.get_version_num() != 4 || id.to_string() != document.value {
                return Err(RoundError::Protocol("DocumentId is invalid"));
            }
            Ok(ReplicaObject::Document(id))
        }
        None => Err(RoundError::Protocol("replica object reference is empty")),
    }
}

pub(super) fn object_summary_to_proto(summary: &ReplicaObjectSummary) -> ProtoObjectSummary {
    ProtoObjectSummary {
        object: Some(replica_object_to_proto(summary.object)),
        loro_version_vector: Some(version_vector_to_proto(&summary.version_vector)),
        loro_frontier: Some(frontier_to_proto(&summary.frontier)),
    }
}

pub(super) fn object_summary_from_proto(
    summary: ProtoObjectSummary,
) -> Result<ReplicaObjectSummary, RoundError> {
    Ok(ReplicaObjectSummary {
        object: summary
            .object
            .as_ref()
            .ok_or(RoundError::Protocol("object summary is missing its object"))
            .and_then(replica_object_from_proto)?,
        version_vector: version_vector_from_proto(summary.loro_version_vector.as_ref())?,
        frontier: frontier_from_proto(summary.loro_frontier.as_ref())?,
    })
}

pub(super) fn version_vector_to_proto(vector: &VersionVector) -> LoroVersionVector {
    let mut entries = vector
        .iter()
        .map(|(peer_id, counter)| LoroVersionEntry {
            peer_id: *peer_id,
            counter: *counter,
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.peer_id);
    LoroVersionVector { entries }
}

pub(super) fn version_vector_from_proto(
    vector: Option<&LoroVersionVector>,
) -> Result<VersionVector, RoundError> {
    let entries = vector
        .ok_or(RoundError::Protocol("Loro version vector is missing"))?
        .entries
        .as_slice();
    let mut previous = None;
    let mut decoded = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.counter < 0
            || entry.peer_id == u64::MAX
            || previous.is_some_and(|previous| previous >= entry.peer_id)
        {
            return Err(RoundError::Protocol("Loro version vector is not canonical"));
        }
        previous = Some(entry.peer_id);
        decoded.push((entry.peer_id, entry.counter));
    }
    Ok(decoded.into_iter().collect())
}

pub(super) fn frontier_to_proto(frontier: &Frontiers) -> LoroFrontier {
    let mut ids = frontier
        .iter()
        .map(|id| LoroId {
            peer_id: id.peer,
            counter: id.counter,
        })
        .collect::<Vec<_>>();
    ids.sort_by_key(|id| (id.peer_id, id.counter));
    LoroFrontier { ids }
}

pub(super) fn frontier_from_proto(
    frontier: Option<&LoroFrontier>,
) -> Result<Frontiers, RoundError> {
    let ids = &frontier
        .ok_or(RoundError::Protocol("Loro frontier is missing"))?
        .ids;
    let mut previous = None;
    let mut decoded = Frontiers::default();
    for id in ids {
        if id.counter < 0
            || id.peer_id == u64::MAX
            || previous.is_some_and(|previous| previous >= (id.peer_id, id.counter))
        {
            return Err(RoundError::Protocol("Loro frontier is not canonical"));
        }
        previous = Some((id.peer_id, id.counter));
        decoded.push(ID::new(id.peer_id, id.counter));
    }
    Ok(decoded)
}

pub(super) fn has_updates(source: &VersionVector, receiver: &VersionVector) -> bool {
    source
        .iter()
        .any(|(peer, counter)| *counter > receiver.get(peer).copied().unwrap_or_default())
}

pub(super) fn version_vector_covers(candidate: &VersionVector, required: &VersionVector) -> bool {
    required
        .iter()
        .all(|(peer, counter)| candidate.get(peer).copied().unwrap_or_default() >= *counter)
}

pub(super) fn chunk_count(size: usize, chunk_bytes: u32) -> Result<u32, RoundError> {
    expected_chunk_count(
        u64::try_from(size).map_err(|_| RoundError::Protocol("transfer size overflowed"))?,
        chunk_bytes,
    )
}

pub(super) fn expected_chunk_count(size: u64, chunk_bytes: u32) -> Result<u32, RoundError> {
    if chunk_bytes == 0 {
        return Err(RoundError::Protocol("negotiated chunk size is zero"));
    }
    let chunks = if size == 0 {
        0
    } else {
        size.saturating_add(u64::from(chunk_bytes) - 1) / u64::from(chunk_bytes)
    };
    u32::try_from(chunks).map_err(|_| RoundError::Protocol("transfer has too many chunks"))
}

pub(super) fn decode_sha256(value: &str) -> Result<Vec<u8>, RoundError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(RoundError::Protocol(
            "SHA-256 value is not canonical lower hex",
        ));
    }
    (0..32)
        .map(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| RoundError::Protocol("SHA-256 value is invalid"))
        })
        .collect()
}

pub(super) fn parse_round_id(value: &str) -> Option<Uuid> {
    let id = Uuid::parse_str(value).ok()?;
    (id.get_version_num() == 4 && id.to_string() == value).then_some(id)
}
