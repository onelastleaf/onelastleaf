use std::collections::{BTreeMap, BTreeSet};

use loro::{Container, ContainerType, LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use super::{
    super::{
        ReplicaError,
        types::{BinaryStamp, BinaryVersion, parse_uuid_v4},
    },
    loro::{map_string, map_u64_string},
    support::{snapshot_from_store_error, validate_sha256},
};

pub(super) fn decode_binary_versions(
    record: &LoroMap,
) -> Result<BTreeMap<BinaryStamp, BinaryVersion>, ReplicaError> {
    let Some(ValueOrContainer::Container(Container::Map(versions))) = record.get("binary_versions")
    else {
        return Err(ReplicaError::InvalidSnapshot(
            "binary entry has no binary_versions LoroMap".to_owned(),
        ));
    };
    let mut decoded = BTreeMap::new();
    let mut decode_error = None;
    versions.for_each(|key, value| {
        if decode_error.is_some() {
            return;
        }
        let result = (|| {
            let ValueOrContainer::Container(Container::Map(version)) = value else {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version is not a LoroMap".to_owned(),
                ));
            };
            require_exact_map_fields(
                &version,
                &[
                    "lamport_clock",
                    "writer_node_id",
                    "sha256",
                    "size_bytes",
                    "media_type",
                ],
                "binary version",
            )?;
            let stamp = BinaryStamp {
                lamport_clock: map_u64_string(&version, "lamport_clock")?,
                writer_node_id: parse_uuid_v4(
                    &map_string(&version, "writer_node_id")?,
                    "writer_node_id",
                )
                .map_err(snapshot_from_store_error)?,
            };
            if key != format!("{}@{}", stamp.lamport_clock, stamp.writer_node_id) {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version key contradicts its stamp".to_owned(),
                ));
            }
            let sha256 = map_string(&version, "sha256")?;
            validate_sha256(&sha256)?;
            let value = BinaryVersion {
                sha256,
                size_bytes: map_u64_string(&version, "size_bytes")?,
                media_type: map_string(&version, "media_type")?,
            };
            if decoded.insert(stamp, value).is_some() {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version stamp is duplicated".to_owned(),
                ));
            }
            Ok(())
        })();
        if let Err(error) = result {
            decode_error = Some(error);
        }
    });
    if let Some(error) = decode_error {
        return Err(error);
    }
    if decoded.is_empty() {
        return Err(ReplicaError::InvalidSnapshot(
            "binary entry has no retained version".to_owned(),
        ));
    }
    Ok(decoded)
}

pub(super) fn validate_root_schema(
    doc: &LoroDoc,
    expected: &[(&str, ContainerType)],
    object: &str,
) -> Result<(), ReplicaError> {
    let value = doc.get_value();
    let roots = value.as_map().ok_or_else(|| {
        ReplicaError::InvalidSnapshot(format!("{object} Loro roots are not a map"))
    })?;
    if roots.len() != expected.len() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "{object} Loro snapshot has an unexpected root set"
        )));
    }
    for (name, expected_type) in expected {
        let Some(LoroValue::Container(container_id)) = roots.get(*name) else {
            return Err(ReplicaError::InvalidSnapshot(format!(
                "{object} Loro root {name} is missing"
            )));
        };
        if container_id.container_type() != *expected_type {
            return Err(ReplicaError::InvalidSnapshot(format!(
                "{object} Loro root {name} has the wrong container type"
            )));
        }
    }
    Ok(())
}

pub(super) fn require_exact_map_fields(
    map: &LoroMap,
    expected: &[&str],
    object: &str,
) -> Result<(), ReplicaError> {
    let mut actual = BTreeSet::new();
    map.for_each(|key, _| {
        actual.insert(key.to_owned());
    });
    let expected = expected
        .iter()
        .map(|field| (*field).to_owned())
        .collect::<BTreeSet<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(ReplicaError::InvalidSnapshot(format!(
            "{object} fields do not match its schema"
        )))
    }
}

pub(super) fn invalid_snapshot(message: &str) -> ReplicaError {
    ReplicaError::InvalidSnapshot(message.to_owned())
}
