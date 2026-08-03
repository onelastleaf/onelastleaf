pub(super) const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS oll_meta (
        singleton BIGINT PRIMARY KEY,
        active_generation TEXT,
        projection_pending BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS replica_generations (
        generation_id TEXT PRIMARY KEY,
        replica_id TEXT NOT NULL,
        loro_peer_id TEXT NOT NULL,
        root_catalog_node_id TEXT NOT NULL,
        catalog_loro BYTEA NOT NULL,
        lamport_clock TEXT NOT NULL,
        projection_generation TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS generation_state_tokens (
        generation_id TEXT PRIMARY KEY,
        state_token BYTEA NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS active_state_guard (
        singleton BIGINT PRIMARY KEY,
        generation_id TEXT NOT NULL,
        state_token BYTEA NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS catalog_entries (
        generation_id TEXT NOT NULL,
        catalog_node_id TEXT NOT NULL,
        parent_catalog_node_id TEXT NOT NULL,
        loro_tree_id TEXT NOT NULL,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,
        deleted BIGINT NOT NULL,
        catalog_revision BYTEA NOT NULL,
        document_id TEXT,
        binary_id TEXT,
        media_type TEXT,
        encoding TEXT,
        has_bom BIGINT,
        size_bytes TEXT,
        PRIMARY KEY (generation_id, catalog_node_id)
    )",
    "CREATE TABLE IF NOT EXISTS document_objects (
        generation_id TEXT NOT NULL,
        document_id TEXT NOT NULL,
        loro BYTEA NOT NULL,
        revision BYTEA NOT NULL,
        PRIMARY KEY (generation_id, document_id)
    )",
    "CREATE TABLE IF NOT EXISTS binary_versions (
        generation_id TEXT NOT NULL,
        binary_id TEXT NOT NULL,
        lamport_clock TEXT NOT NULL,
        writer_node_id TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        size_bytes TEXT NOT NULL,
        media_type TEXT NOT NULL,
        PRIMARY KEY (
            generation_id,
            binary_id,
            lamport_clock,
            writer_node_id
        )
    )",
    "CREATE TABLE IF NOT EXISTS blobs (
        sha256 TEXT PRIMARY KEY,
        size_bytes TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS blob_chunks (
        sha256 TEXT NOT NULL,
        chunk_index BIGINT NOT NULL,
        data BYTEA NOT NULL,
        PRIMARY KEY (sha256, chunk_index)
    )",
    "CREATE TABLE IF NOT EXISTS replica_operations (
        generation_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        source TEXT NOT NULL,
        kind TEXT NOT NULL,
        catalog_node_id TEXT NOT NULL,
        document_id TEXT NOT NULL,
        path_before TEXT,
        path_after TEXT,
        correlation_id TEXT NOT NULL,
        PRIMARY KEY (generation_id, operation_id, document_id)
    )",
    "CREATE INDEX IF NOT EXISTS replica_operations_document_time
        ON replica_operations (generation_id, document_id, timestamp)",
    "CREATE TABLE IF NOT EXISTS retained_commits (
        generation_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        request BYTEA NOT NULL,
        response BYTEA NOT NULL,
        PRIMARY KEY (generation_id, operation_id)
    )",
    "CREATE TABLE IF NOT EXISTS projection_paths (
        generation_id TEXT NOT NULL,
        path TEXT NOT NULL,
        PRIMARY KEY (generation_id, path)
    )",
    "CREATE TABLE IF NOT EXISTS replica_identity_transition (
        singleton BIGINT PRIMARY KEY,
        transition_kind TEXT NOT NULL,
        expected_active_generation TEXT,
        candidate_generation TEXT NOT NULL,
        old_replica_id TEXT,
        new_replica_id TEXT NOT NULL,
        old_identity_file BYTEA,
        new_identity_file BYTEA NOT NULL,
        projection_pending BIGINT NOT NULL,
        committed BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS bootstrap_claim (
        singleton BIGINT PRIMARY KEY,
        claim_id TEXT NOT NULL,
        source_node_id TEXT NOT NULL,
        correlation_id TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS sync_peer_bindings (
        node_id TEXT PRIMARY KEY,
        node_name TEXT NOT NULL UNIQUE
    )",
    "CREATE TABLE IF NOT EXISTS sync_connect_bindings (
        connect_target TEXT PRIMARY KEY,
        node_id TEXT NOT NULL
    )",
];
