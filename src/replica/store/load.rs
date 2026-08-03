use std::collections::BTreeMap;

use sqlx::Row;

use super::{
    super::{
        ReplicaError,
        model::validate_loaded_replica,
        types::{
            ActiveReplica, BinaryEntry, BinaryStamp, BinaryVersion, CatalogEntry, DocumentEntry,
            DocumentObject, EntryData, parse_uuid_v4,
        },
    },
    ReplicaStore,
    support::{
        kind_fields_error, parse_bool, parse_optional_uuid, parse_u64, revision_array, store_error,
        validate_blob_hash,
    },
};

impl ReplicaStore {
    pub async fn load_generation(&self, generation: &str) -> Result<ActiveReplica, ReplicaError> {
        let row = sqlx::query(
            "SELECT generation_id, replica_id, loro_peer_id,
                    root_catalog_node_id, catalog_loro, lamport_clock,
                    projection_generation
             FROM replica_generations
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ReplicaError::CorruptStore("active replica generation is missing".to_owned())
        })?;
        let generation_id = parse_uuid_v4(
            &row.try_get::<String, _>("generation_id")
                .map_err(store_error)?,
            "generation_id",
        )?;
        let replica_id = parse_uuid_v4(
            &row.try_get::<String, _>("replica_id")
                .map_err(store_error)?,
            "replica_id",
        )?;
        let root_catalog_node_id = parse_uuid_v4(
            &row.try_get::<String, _>("root_catalog_node_id")
                .map_err(store_error)?,
            "root_catalog_node_id",
        )?;
        let loro_peer_id = parse_u64(
            &row.try_get::<String, _>("loro_peer_id")
                .map_err(store_error)?,
            "loro_peer_id",
        )?;
        if loro_peer_id == u64::MAX {
            return Err(ReplicaError::CorruptStore(
                "loro_peer_id uses Loro's reserved root identity".to_owned(),
            ));
        }
        let lamport_clock = parse_u64(
            &row.try_get::<String, _>("lamport_clock")
                .map_err(store_error)?,
            "lamport_clock",
        )?;
        let projection_generation = parse_u64(
            &row.try_get::<String, _>("projection_generation")
                .map_err(store_error)?,
            "projection_generation",
        )?;
        let catalog_loro = row
            .try_get::<Vec<u8>, _>("catalog_loro")
            .map_err(store_error)?;

        let mut entries = BTreeMap::new();
        let rows = sqlx::query(
            "SELECT catalog_node_id, parent_catalog_node_id, loro_tree_id,
                    name, kind, deleted, catalog_revision, document_id,
                    binary_id, media_type, encoding, has_bom, size_bytes
             FROM catalog_entries
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        for row in rows {
            let catalog_node_id = parse_uuid_v4(
                &row.try_get::<String, _>("catalog_node_id")
                    .map_err(store_error)?,
                "catalog_node_id",
            )?;
            let parent_catalog_node_id = parse_uuid_v4(
                &row.try_get::<String, _>("parent_catalog_node_id")
                    .map_err(store_error)?,
                "parent_catalog_node_id",
            )?;
            let kind = row.try_get::<String, _>("kind").map_err(store_error)?;
            let media_type = row
                .try_get::<Option<String>, _>("media_type")
                .map_err(store_error)?;
            let document_id = row
                .try_get::<Option<String>, _>("document_id")
                .map_err(store_error)?;
            let binary_id = row
                .try_get::<Option<String>, _>("binary_id")
                .map_err(store_error)?;
            let encoding = row
                .try_get::<Option<String>, _>("encoding")
                .map_err(store_error)?;
            let has_bom = row
                .try_get::<Option<i64>, _>("has_bom")
                .map_err(store_error)?;
            let size_bytes = row
                .try_get::<Option<String>, _>("size_bytes")
                .map_err(store_error)?;
            let data = match kind.as_str() {
                "directory" => {
                    if media_type.is_some()
                        || document_id.is_some()
                        || binary_id.is_some()
                        || encoding.is_some()
                        || has_bom.is_some()
                        || size_bytes.is_some()
                    {
                        return Err(kind_fields_error());
                    }
                    EntryData::Directory
                }
                "document" => EntryData::Document(DocumentEntry {
                    document_id: if binary_id.is_none() {
                        parse_optional_uuid(document_id, "document_id")?
                            .ok_or_else(kind_fields_error)?
                    } else {
                        return Err(kind_fields_error());
                    },
                    media_type: media_type.ok_or_else(kind_fields_error)?,
                    encoding: encoding.ok_or_else(kind_fields_error)?,
                    has_byte_order_mark: parse_bool(
                        has_bom.ok_or_else(kind_fields_error)?,
                        "has_bom",
                    )?,
                    size_bytes: parse_u64(
                        &size_bytes.ok_or_else(kind_fields_error)?,
                        "size_bytes",
                    )?,
                }),
                "binary" => {
                    if document_id.is_some()
                        || encoding.is_some()
                        || has_bom.is_some()
                        || size_bytes.is_none()
                    {
                        return Err(kind_fields_error());
                    }
                    EntryData::Binary(BinaryEntry {
                        binary_id: parse_optional_uuid(binary_id, "binary_id")?
                            .ok_or_else(kind_fields_error)?,
                        media_type: media_type.ok_or_else(kind_fields_error)?,
                        versions: BTreeMap::new(),
                    })
                }
                _ => {
                    return Err(ReplicaError::CorruptStore(
                        "catalog entry has an unknown kind".to_owned(),
                    ));
                }
            };
            let entry = CatalogEntry {
                catalog_node_id,
                parent_catalog_node_id,
                loro_tree_id: row
                    .try_get::<String, _>("loro_tree_id")
                    .map_err(store_error)?,
                name: row.try_get::<String, _>("name").map_err(store_error)?,
                deleted: parse_bool(
                    row.try_get::<i64, _>("deleted").map_err(store_error)?,
                    "deleted",
                )?,
                catalog_revision: revision_array(
                    row.try_get::<Vec<u8>, _>("catalog_revision")
                        .map_err(store_error)?,
                    "catalog_revision",
                )?,
                data,
            };
            if entries.insert(catalog_node_id, entry).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate catalog_node_id".to_owned(),
                ));
            }
        }

        let rows = sqlx::query(
            "SELECT binary_id, lamport_clock, writer_node_id, sha256,
                    size_bytes, media_type
             FROM binary_versions
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        for row in rows {
            let binary_id = parse_uuid_v4(
                &row.try_get::<String, _>("binary_id").map_err(store_error)?,
                "binary_id",
            )?;
            let stamp = BinaryStamp {
                lamport_clock: parse_u64(
                    &row.try_get::<String, _>("lamport_clock")
                        .map_err(store_error)?,
                    "lamport_clock",
                )?,
                writer_node_id: parse_uuid_v4(
                    &row.try_get::<String, _>("writer_node_id")
                        .map_err(store_error)?,
                    "writer_node_id",
                )?,
            };
            let version = BinaryVersion {
                sha256: row.try_get::<String, _>("sha256").map_err(store_error)?,
                size_bytes: parse_u64(
                    &row.try_get::<String, _>("size_bytes")
                        .map_err(store_error)?,
                    "size_bytes",
                )?,
                media_type: row
                    .try_get::<String, _>("media_type")
                    .map_err(store_error)?,
            };
            let entry = entries
                .values_mut()
                .find(|entry| {
                    entry
                        .binary()
                        .is_some_and(|binary| binary.binary_id == binary_id)
                })
                .ok_or_else(|| {
                    ReplicaError::CorruptStore(
                        "binary version has no matching catalog entry".to_owned(),
                    )
                })?;
            let EntryData::Binary(binary) = &mut entry.data else {
                unreachable!()
            };
            if binary.versions.insert(stamp, version).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate binary version stamp".to_owned(),
                ));
            }
        }

        let rows = sqlx::query(
            "SELECT document_id, loro, revision
             FROM document_objects
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        let mut documents = BTreeMap::new();
        for row in rows {
            let document_id = parse_uuid_v4(
                &row.try_get::<String, _>("document_id")
                    .map_err(store_error)?,
                "document_id",
            )?;
            let document = DocumentObject {
                document_id,
                loro: row.try_get::<Vec<u8>, _>("loro").map_err(store_error)?,
                revision: revision_array(
                    row.try_get::<Vec<u8>, _>("revision").map_err(store_error)?,
                    "document_revision",
                )?,
            };
            if documents.insert(document_id, document).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate document_id".to_owned(),
                ));
            }
        }
        for entry in entries.values() {
            if let Some(document) = entry.document()
                && !documents.contains_key(&document.document_id)
            {
                return Err(ReplicaError::CorruptStore(
                    "catalog document has no retained Loro object".to_owned(),
                ));
            }
            if let Some(binary) = entry.binary()
                && binary.versions.is_empty()
            {
                return Err(ReplicaError::CorruptStore(
                    "catalog binary has no retained version".to_owned(),
                ));
            }
        }

        let replica = ActiveReplica {
            generation_id,
            replica_id,
            loro_peer_id,
            root_catalog_node_id,
            catalog_loro,
            lamport_clock,
            projection_generation,
            entries,
            documents,
        };
        validate_loaded_replica(&replica)?;
        for binary in replica.entries.values().filter_map(CatalogEntry::binary) {
            for version in binary.versions.values() {
                validate_blob_hash(&version.sha256)?;
                if self.blob_size(&version.sha256).await? != version.size_bytes {
                    return Err(ReplicaError::CorruptStore(
                        "binary version size differs from its blob metadata".to_owned(),
                    ));
                }
            }
        }
        Ok(replica)
    }
}
