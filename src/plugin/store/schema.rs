use sqlx::AnyPool;

pub(super) const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS plugin_meta (
        singleton BIGINT PRIMARY KEY,
        artifact_download_dir BYTEA
    )",
    "CREATE TABLE IF NOT EXISTS plugins (
        plugin_id TEXT PRIMARY KEY,
        plugin_name TEXT NOT NULL UNIQUE,
        normalized_declaration BYTEA NOT NULL,
        declaration_sha256 BYTEA NOT NULL,
        effective_manifest BYTEA NOT NULL,
        selected_commit TEXT,
        install_mode TEXT NOT NULL,
        release_id TEXT,
        current_generation TEXT NOT NULL,
        running_generation TEXT,
        running_instance_id TEXT,
        desired_state TEXT NOT NULL,
        restart_sequence BIGINT NOT NULL,
        consumed_restart_sequence BIGINT NOT NULL,
        restart_attempt BIGINT NOT NULL,
        restart_not_before_seconds BIGINT,
        restart_not_before_nanos BIGINT,
        last_lifecycle_failure TEXT
    )",
    "CREATE TABLE IF NOT EXISTS plugin_package_publish_intents (
        plugin_id TEXT PRIMARY KEY,
        plugin_name TEXT NOT NULL UNIQUE,
        operation_id TEXT NOT NULL,
        expected_current_generation TEXT,
        candidate_generation TEXT NOT NULL,
        normalized_declaration BYTEA NOT NULL,
        declaration_sha256 BYTEA NOT NULL,
        effective_manifest BYTEA NOT NULL,
        selected_commit TEXT,
        install_mode TEXT NOT NULL,
        release_id TEXT,
        correlation_id TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS plugin_removal_intents (
        plugin_id TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL,
        plugins_lua_sha256 BYTEA NOT NULL,
        prepared_plugins_lua BYTEA NOT NULL,
        trash_path BYTEA NOT NULL,
        phase TEXT NOT NULL,
        correlation_id TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS plugin_jobs (
        job_id TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL UNIQUE,
        plugin_id TEXT NOT NULL,
        normalized_payload BYTEA NOT NULL,
        action TEXT NOT NULL,
        arguments BYTEA NOT NULL,
        deadline_kind TEXT NOT NULL,
        explicit_deadline_seconds BIGINT,
        explicit_deadline_nanos BIGINT,
        absolute_deadline_seconds BIGINT NOT NULL,
        absolute_deadline_nanos BIGINT NOT NULL,
        state TEXT NOT NULL,
        cancellation_reason TEXT,
        plugin_instance_id TEXT NOT NULL,
        admitted_at_seconds BIGINT NOT NULL,
        admitted_at_nanos BIGINT NOT NULL,
        accepted_at_seconds BIGINT,
        accepted_at_nanos BIGINT,
        terminal_at_seconds BIGINT,
        terminal_at_nanos BIGINT,
        updated_at_seconds BIGINT NOT NULL,
        updated_at_nanos BIGINT NOT NULL,
        correlation_id TEXT NOT NULL,
        result BYTEA,
        error_code TEXT,
        error_message TEXT
    )",
    "CREATE INDEX IF NOT EXISTS plugin_jobs_plugin_time
        ON plugin_jobs (plugin_id, admitted_at_seconds, admitted_at_nanos)",
    "CREATE TABLE IF NOT EXISTS plugin_artifacts (
        artifact_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        plugin_id TEXT NOT NULL,
        file_name TEXT NOT NULL,
        media_type TEXT NOT NULL,
        size_bytes TEXT NOT NULL,
        sha256 BYTEA NOT NULL,
        destination BYTEA NOT NULL,
        stored_at_seconds BIGINT NOT NULL,
        stored_at_nanos BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS plugin_artifacts_job
        ON plugin_artifacts (job_id)",
    "CREATE TABLE IF NOT EXISTS plugin_artifact_publish_intents (
        artifact_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        plugin_id TEXT NOT NULL,
        file_name TEXT NOT NULL,
        media_type TEXT NOT NULL,
        size_bytes TEXT NOT NULL,
        sha256 BYTEA NOT NULL,
        staging_path BYTEA NOT NULL,
        destination BYTEA NOT NULL,
        correlation_id TEXT NOT NULL
    )",
];

pub(super) async fn migrate_existing_tables(pool: &AnyPool) -> Result<(), sqlx::Error> {
    const JOB_TIMESTAMP_COLUMNS: &[(&str, &str)] = &[
        (
            "SELECT accepted_at_seconds FROM plugin_jobs WHERE 1 = 0",
            "ALTER TABLE plugin_jobs ADD COLUMN accepted_at_seconds BIGINT",
        ),
        (
            "SELECT accepted_at_nanos FROM plugin_jobs WHERE 1 = 0",
            "ALTER TABLE plugin_jobs ADD COLUMN accepted_at_nanos BIGINT",
        ),
        (
            "SELECT terminal_at_seconds FROM plugin_jobs WHERE 1 = 0",
            "ALTER TABLE plugin_jobs ADD COLUMN terminal_at_seconds BIGINT",
        ),
        (
            "SELECT terminal_at_nanos FROM plugin_jobs WHERE 1 = 0",
            "ALTER TABLE plugin_jobs ADD COLUMN terminal_at_nanos BIGINT",
        ),
    ];

    // Probe each nullable column independently so an interrupted migration is
    // safely resumed without manufacturing timestamps for pre-existing jobs.
    for (probe, migration) in JOB_TIMESTAMP_COLUMNS {
        if sqlx::query(*probe).fetch_optional(pool).await.is_err() {
            sqlx::query(*migration).execute(pool).await?;
        }
    }
    Ok(())
}
