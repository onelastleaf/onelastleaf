use std::collections::BTreeSet;

use getrandom::fill as fill_random;
use loro::{Container, LoroDoc, LoroMap, LoroValue, TreeID, ValueOrContainer};
use uuid::Uuid;

use super::{super::ReplicaError, support::loro_error};

pub fn generate_loro_peer_id(excluded: &BTreeSet<u64>) -> Result<u64, ReplicaError> {
    loop {
        let mut bytes = [0_u8; 8];
        fill_random(&mut bytes).map_err(|error| {
            ReplicaError::Internal(format!("cannot generate Loro peer identity: {error}"))
        })?;
        let peer = u64::from_ne_bytes(bytes);
        if peer != u64::MAX && !excluded.contains(&peer) {
            return Ok(peer);
        }
    }
}

pub(crate) fn new_loro_doc(peer: u64) -> Result<LoroDoc, ReplicaError> {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer).map_err(loro_error)?;
    Ok(doc)
}

pub(crate) fn import_loro_doc(bytes: &[u8], peer: u64) -> Result<LoroDoc, ReplicaError> {
    let doc = new_loro_doc(peer)?;
    doc.import(bytes).map_err(loro_error)?;
    Ok(doc)
}

pub(crate) fn get_entry_record(entries: &LoroMap, id: Uuid) -> Result<LoroMap, ReplicaError> {
    match entries.get(&id.to_string()) {
        Some(ValueOrContainer::Container(Container::Map(map))) => Ok(map),
        Some(_) => Err(ReplicaError::CorruptStore(
            "catalog entry record is not a LoroMap".to_owned(),
        )),
        None => Err(ReplicaError::CorruptStore(
            "catalog entry record is missing".to_owned(),
        )),
    }
}

pub(super) fn map_string(map: &LoroMap, key: &'static str) -> Result<String, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::String(value))) if !value.is_empty() => {
            Ok(value.to_string())
        }
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be a non-empty string"
        ))),
    }
}

pub(super) fn map_i64(map: &LoroMap, key: &'static str) -> Result<i64, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::I64(value))) => Ok(value),
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be an integer"
        ))),
    }
}

pub(super) fn map_bool(map: &LoroMap, key: &'static str) -> Result<bool, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::Bool(value))) => Ok(value),
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be boolean"
        ))),
    }
}

pub(super) fn map_u64_string(map: &LoroMap, key: &'static str) -> Result<u64, ReplicaError> {
    map_string(map, key)?.parse().map_err(|_| {
        ReplicaError::InvalidSnapshot(format!("catalog field {key} must be a u64 string"))
    })
}

pub(crate) fn parse_tree_id(value: &str) -> Result<TreeID, ReplicaError> {
    TreeID::try_from(value)
        .map_err(|_| ReplicaError::CorruptStore("invalid LoroTree node ID".to_owned()))
}
