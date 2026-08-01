# Observability and logging

## Scope

The first implementation uses structured logs as its primary observability
mechanism. Metrics and distributed tracing systems are optional future sinks;
they are not required before the logging contract is complete.

Logs are part of feature correctness, especially for synchronization, process
lifecycle, working-tree reconciliation, replica-store recovery, snapshot import,
and plugin jobs. New workflows MUST define useful start, success, failure,
duration, and identity fields rather than emitting only free-form error strings.

## Location and ownership

oll is a user-level daemon. Daemon logs live under the deployment's log
directory, whose default is the platform state directory plus `oll`. On Linux
that is:

```text
$XDG_STATE_HOME/oll/
```

When `XDG_STATE_HOME` is unset, the Linux default is
`$HOME/.local/state/oll/`; the platform directory helper supplies the Darwin
location. `oll init --log-dir`, `oll run --log-dir`, and `OLL_LOG_DIR` select
another directory according to `cli.md`.

oll creates the directory and files as the user running the deployment:

```text
<log-dir>/           0700 <user>:<primary-group>
├── oll.log          0600 <user>:<primary-group>
├── sync.log         0600 <user>:<primary-group>
└── plugin.log       0600 <user>:<primary-group>  # plugin stage
```

oll does not require a system `oll` account, a shared log group, root
privileges, or pre-created `/var/log` state. Existing roots and files with unsafe
ownership, type, or permissions are rejected rather than silently relaxed.

Early startup failures that occur before file logging is ready are emitted to
stderr. In daemon mode, failure to initialize the required log files is a startup
error rather than a silent loss of observability.

## File routing

### `oll.log`

`oll.log` is the high-signal daemon timeline. It contains:

- startup, validated configuration summary, and shutdown;
- node and replica initialization;
- replica-store recovery, projection, and persistence failures;
- filesystem scan/reconciliation and catalog/document/binary operation summaries;
- snapshot export/import/replace lifecycle;
- sync connection, disconnection, retry, and transfer summaries;
- plugin process/job lifecycle;
- scheduler state, panics, and unrecoverable errors.

It MUST NOT contain per-chunk sync traces or raw payloads.

### `sync.log`

`sync.log` contains the detailed network and replica-object synchronization
stream:

- listen/connect addresses, DNS, TCP, TLS, HTTP/2, and gRPC failures;
- local and peer `NodeName`, `NodeId`, `ReplicaId`, and connection identity;
- `SyncHello`/`SyncReady` results and schema mismatches;
- catalog/document advertisements, delta requests, and binary-blob requests;
- snapshot fallback decisions;
- object, transfer, chunk-count, byte-count, duration, and flow-credit fields;
- payload checksum/decompression failure;
- Loro decode/import result and frontier/version summary;
- binary blob verification, byte counts, and pending/materialized state;
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

## Delivery and backpressure

Daemon tasks MUST NOT write, flush, rotate, compress, or synchronize log files
themselves. `NodeLogger::emit` serializes and attempts to enqueue an event into
a bounded in-process queue; one dedicated writer thread owns the sinks and
preserves retained-event queue order while performing all file I/O.

The initial queue holds 4096 events. The writer buffers file output and flushes
it to the operating system after either 256 queued events or 250 milliseconds.
An ordinary batch flush does not call `fsync`/`fdatasync`: these logs are
diagnostic records, not authoritative replica state. Rotation flushes and
synchronizes the file before its atomic rename. Graceful shutdown enqueues a
barrier after the final lifecycle event and requests a durable flush, but waits
only until the node's existing absolute shutdown deadline. A stalled log device
therefore cannot extend daemon shutdown or deployment-lock ownership.

The bounded queue is an intentional trade-off. A synchronous writer would
propagate slow-disk latency into Admin, watcher, SQL, and projection work; an
unbounded asynchronous queue could exhaust memory while a sink is stalled; and
batching flushes without moving writes off the caller would still block on file
writes, rotation, and the sink lock. When the bounded queue is full, `emit`
drops the event instead of waiting or performing synchronous fallback, even for
`ERROR`. Once the writer can make progress, it emits a structured
`log_events_dropped` warning with the dropped count and queue capacity. Runtime
sink failures are reported on stderr because logging a sink failure back into
the same sink would recurse. Startup still fails if the required directory and
files cannot be safely opened.

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
  "node_id": "9ba4a1aa-4c7d-4b11-b902-3155cf8ca5f3",
  "peer_node_id": "44d62c47-0d82-42f0-a767-e3d6d5e75858",
  "replica_id": "f00cb07c-d513-4399-9c3f-9cf947d81945",
  "document_id": "60c8b0de-1d43-4f48-9a9c-13b7d19af3b4",
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

- `node_name`, `node_id`, `peer_node_name`, `peer_node_id`, and `replica_id`;
- `catalog_node_id`, `document_id`, `binary_id`, and sanitized `path`;
- `connection_id`, `transfer_id`, and `message_id`;
- `plugin_instance_id`, `job_id`, `task_id`, and `task_group_id`;
- `plugin_desired_state`, `plugin_process_state`, `exit_status`, `signal`,
  `restart_reason`, and `restart_attempt`;
- `parent_call_id`, `call_depth`, and `causal_depth`;
- `operation_id`, `snapshot_id`, and `artifact_id`;
- `error_code`, `retryable`, `attempt`, and `backoff_ms`;
- `bytes`, `object_count`, `chunk_count`, and `duration_ms`.

Each delayed working-tree projection retry is a structured `WARN` event with a
sanitized namespace `path`, the one-based `attempt` that failed, the
`error_code`, and `backoff_ms`. The final operation or recovery failure remains
a separate `ERROR` event under the same correlation ID; neither event includes
file contents.

For host-ingested plugin events, `timestamp` is the plugin emission time and
`observed_at` is the host receive time. Host events may omit `observed_at` when
it is identical to `timestamp`.

## Correlation IDs

Correlation IDs are mandatory, not an optional logging enhancement.

- A new external operation without an ID receives a collision-resistant ID at
  the first oll boundary.
- Every child call, task, log, and artifact transfer inherits the current
  `correlation_id`.
- An Admin snapshot export or import, its replica-layer lifecycle events, its
  blocking archive work, and any snapshot-import operation records inherit the
  `AdminCallContext` correlation ID without generating a replacement at the
  replica boundary.
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

The control path is `oll log set <target>=<level>`, for example:

```text
oll log set oll::sync=trace
```

The CLI accepts one directive, validates an `oll` tracing target made of
ASCII identifier segments separated by `::`, parses the lower-case level
(`error`, `warn`, `info`, `debug`, or `trace`), and sends a typed
`SetLogFilter` Admin request. The daemon applies that target filter to live
events while preserving normal sink routing. It is intentionally not persisted
to `config.lua` or `node.json`: a restart restores the production defaults
above. Invalid target syntax is a CLI error; an invalid typed request is an
Admin `INVALID_ARGUMENT` error.

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
- raw Loro update/snapshot bytes, binary blob bytes, or complete protobuf
  payloads;
- Lua configuration values, prompts, credentials, or AI tokens;
- HTTP authorization, cookies, private keys, or plugin secrets;
- artifact contents.

Network addresses and sanitized document paths may be logged because they are
needed for diagnosis, but file permissions remain non-world-readable. Error
objects must be redacted before structured serialization, not afterward by a
text filter.

## Rotation and retention

Rotation is managed by oll inside the user-owned log directory. Rotation and
reopen must preserve the directory and file permissions above and use atomic
renames; it must not require a system `logrotate` installation or privileged
signal.

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
- non-blocking bounded-queue behavior and a structured dropped-event summary;
- batch/periodic visibility and deadline-bounded final draining;
- required field presence and stable event names;
- correlation propagation through async tasks, sync envelopes, plugin calls,
  jobs, and scheduler callbacks;
- redaction of representative secrets and document content;
- correct sink routing and limited duplication;
- rotation/reopen behavior;
- useful structured context on working-tree reconciliation, network, import,
  projection, and recovery failures;
- plugin desired-state persistence, process-state transitions, exit detection,
  restart decisions, backoff, and graceful-to-signal escalation.
