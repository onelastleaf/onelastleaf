# Observability and logging

## Scope

The first implementation uses structured logs as its primary observability
mechanism. Metrics and distributed tracing systems are optional future sinks;
they are not required before the logging contract is complete.

Logs are part of feature correctness, especially for synchronization, process
lifecycle, persistence recovery, snapshot import, and plugin jobs. New workflows
MUST define useful start, success, failure, duration, and identity fields rather
than emitting only free-form error strings.

## Location and ownership

Daemon logs live under:

```text
/var/log/oll/
```

The path follows the executable and service name `oll`; the project does not use
`/var/log/onelastleaf/`.

The package/service setup creates the directory and files:

```text
/var/log/oll/       0750 oll:oll
├── oll.log         0640 oll:oll
├── sync.log        0640 oll:oll
└── plugin.log      0640 oll:oll  # created with the plugin system
```

Early startup failures that occur before file logging is ready are emitted to
stderr. In daemon mode, failure to initialize the required log files is a startup
error rather than a silent loss of observability.

## File routing

### `oll.log`

`oll.log` is the high-signal daemon timeline. It contains:

- startup, validated configuration summary, and shutdown;
- node and replica initialization;
- recovery and persistence failures;
- catalog/document operation summaries;
- snapshot export/import/replace lifecycle;
- sync connection, disconnection, retry, and transfer summaries;
- plugin process/job lifecycle;
- scheduler state, panics, and unrecoverable errors.

It MUST NOT contain per-chunk sync traces or raw payloads.

### `sync.log`

`sync.log` contains the detailed network and replica-object synchronization
stream:

- listen/connect addresses, DNS, TCP, TLS, HTTP/2, and gRPC failures;
- local and peer `NodeId`, `ReplicaId`, and connection identity;
- `SyncHello`/`SyncReady` results and fingerprint mismatches;
- catalog/document advertisements and delta requests;
- snapshot fallback decisions;
- object, transfer, chunk-count, byte-count, duration, and flow-credit fields;
- payload checksum/decompression failure;
- Loro import result and frontier/version summary;
- EOF, retry, backoff, and reconnection.

Normal levels record one event per operation or transfer phase, not one event per
chunk. Individual chunk events are `TRACE` only.

### `plugin.log`

`plugin.log` is added when the plugin system is implemented. It contains plugin
`LogRecord` messages and plugin stdout/stderr wrapped in the host's structured
schema. Host-owned lifecycle events such as desired-state changes, observed
state transitions, spawn, readiness, exit, restart backoff, timeout, shutdown
request, and signal escalation remain in `oll.log`.

## Aggregation model

oll is the local log aggregator. Every daemon component and plugin event is
normalized into one event schema before sink routing.

The system uses limited, deliberate duplication:

- `oll.log` receives high-signal `INFO`, `WARN`, and `ERROR` summaries from all
  modules;
- `sync.log` receives the complete sync target at its configured level;
- `plugin.log` receives plugin-produced output;
- detailed sync/plugin events are not copied wholesale into `oll.log`;
- replica, snapshot, scheduler, and CLI do not each receive separate files.

JSON Lines makes all three files directly ingestible by external tools such as
Fluent Bit, Vector, or journald forwarding without defining an additional oll
log database.

## Event format

Every file is UTF-8 JSON Lines: exactly one JSON object per line, no ANSI escape
codes, and no multiline records. A representative event is:

```json
{
  "timestamp": "2026-07-25T12:00:00.123456Z",
  "observed_at": "2026-07-25T12:00:00.124010Z",
  "level": "INFO",
  "target": "oll::sync",
  "event": "replica_transfer_completed",
  "message": "document update imported",
  "node_id": "node-a",
  "peer_node_id": "node-b",
  "replica_id": "replica-1",
  "document_id": "doc-42",
  "connection_id": "conn-8",
  "transfer_id": "transfer-19",
  "correlation_id": "corr-31",
  "bytes": 1048576,
  "duration_ms": 283
}
```

Required fields are `timestamp`, `level`, `target`, `event`, and
`correlation_id`. `event` is a stable machine-readable snake-case name.
`message` is optional explanatory text and must not be the only representation
of event meaning.

Relevant events SHOULD add typed fields rather than concatenate data into
`message`, including:

- `node_id`, `peer_node_id`, and `replica_id`;
- `catalog_node_id`, `document_id`, and sanitized `path`;
- `connection_id`, `transfer_id`, and `message_id`;
- `plugin_instance_id`, `job_id`, `task_id`, and `task_group_id`;
- `plugin_desired_state`, `plugin_process_state`, `exit_status`, `signal`,
  `restart_reason`, and `restart_attempt`;
- `parent_call_id`, `call_depth`, and `causal_depth`;
- `operation_id`, `snapshot_id`, and `artifact_id`;
- `error_code`, `retryable`, `attempt`, and `backoff_ms`;
- `bytes`, `object_count`, `chunk_count`, and `duration_ms`.

For host-ingested plugin events, `timestamp` is the plugin emission time and
`observed_at` is the host receive time. Host events may omit `observed_at` when
it is identical to `timestamp`.

## Correlation IDs

Correlation IDs are mandatory, not an optional logging enhancement.

- A new external operation without an ID receives a collision-resistant ID at
  the first oll boundary.
- Every child call, task, log, and artifact transfer inherits the current
  `correlation_id`.
- A genuinely independent operation receives a new ID.
- A plugin exit, its restart-backoff decision, the replacement spawn, and the
  replacement session's readiness share one lifecycle correlation ID. The
  triggering Admin command, job operation, or timeout supplies the ID when it
  caused the transition; an unsolicited process or protocol event starts a new
  ID at the supervisor.
- Nested plugin calls additionally set `parent_call_id` and increment
  `call_depth`.
- Derived events and scheduled callbacks preserve the ID and increment or carry
  the documented causal context.
- A sync delta request establishes an ID that is propagated over the wire on
  responses, chunks, import results, and acknowledgements. Both peers therefore
  log the same distributed operation under one ID.
- `connection_id`, `transfer_id`, `job_id`, and `operation_id` complement the
  correlation ID; none replaces it.

Code that spawns an asynchronous task MUST explicitly carry the current tracing
context. Losing context at a Tokio spawn boundary is a correctness bug in
observability.

## Levels and dynamic filtering

Production defaults are:

```text
oll.log:    INFO
sync.log:   INFO
plugin.log: INFO
```

Target-specific filtering can raise `oll::sync` to `DEBUG` or `TRACE` without
restarting the daemon. `DEBUG` records protocol decisions and state summaries;
`TRACE` may record individual frame/chunk metadata but never raw content.

Admin RPC logging follows the same rule. `TRACE` records the method, correlation
ID, duration, outcome, and an allowlisted, field-level-redacted request summary;
it MUST NOT serialize a complete protobuf request. This remains true even though
debug builds may expose gRPC Server Reflection on the local Admin UDS.

`WARN` indicates a recovered or retrying condition. `ERROR` indicates an
operation failure or state requiring intervention. Expected disconnects and
revision conflicts are not automatically errors; their level depends on whether
the surrounding operation failed.

## Sensitive data

No level, including `TRACE`, may record:

- document bodies or selected document text;
- raw Loro update/snapshot bytes or complete protobuf payloads;
- Lua configuration values, prompts, credentials, or AI tokens;
- HTTP authorization, cookies, private keys, or plugin secrets;
- artifact contents.

Network addresses and sanitized document paths may be logged because they are
needed for diagnosis, but file permissions remain non-world-readable. Error
objects must be redacted before structured serialization, not afterward by a
text filter.

## Rotation and retention

Rotation is managed by the operating system's `logrotate` integration. oll MUST
reopen log files after the configured rotation signal.

Initial policy:

- rotate `oll.log` and `plugin.log` daily or at 25 MiB;
- rotate `sync.log` daily or at 100 MiB;
- retain 14 rotations for `oll.log` and 10 for high-volume logs;
- compress rotated logs;
- never block the daemon indefinitely on a slow log sink.

The exact compression program is packaging policy, not an application wire
format.

## Testing

Tests must verify:

- JSON validity and one-event-per-line output;
- required field presence and stable event names;
- correlation propagation through async tasks, sync envelopes, plugin calls,
  jobs, and scheduler callbacks;
- redaction of representative secrets and document content;
- correct sink routing and limited duplication;
- rotation/reopen behavior;
- useful structured context on network, import, and recovery failures.
- plugin desired-state persistence, process-state transitions, exit detection,
  restart decisions, backoff, and graceful-to-signal escalation.
