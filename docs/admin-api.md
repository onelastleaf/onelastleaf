# Local administration API

## Process roles

`oll` is one executable with two process roles. The selected subcommand is the
only role discriminator; implementations MUST NOT introduce a global mode flag
or infer the role from configuration:

| Subcommand | Process role | Behavior |
| --- | --- | --- |
| `oll run` | daemon | Enters the one long-running node runtime and does not exit after startup. |
| `oll init` | bootstrap client | Initializes local configuration, `NodeIdentity`, and the one empty replica slot without starting services. |
| `oll start` | launcher client | Starts a detached `oll run` child, verifies readiness, and exits. |
| `oll psk`, snapshot inspect/verify, log viewing, and `plugin validate` | local client | Generates random output, validates, or reads one local file and exits without Admin. |
| remaining operational subcommands | admin client | Opens the configured Admin API, makes one request under its method-specific deadline policy, renders the result, and exits. |

`init` cannot use an already-running daemon: its purpose includes creating the
state required before the first daemon can start. `start` is also necessarily a
local bootstrap operation, although it probes the Admin API and the
single-instance lock before spawning. These two exceptions are still client
processes and MUST NOT initialize node services in their own process.

## Transport and contract

The Admin API uses gRPC over a Unix domain socket (UDS). It has no TCP port and
MUST NOT listen on a network interface. The first node implementation supports
this Unix transport on Linux and Darwin only. The socket pathname comes from the
same validated configuration root used by the daemon and administrative clients.
oll is a user-level daemon; the default socket is
`<config-root>/run/admin.sock`, inside a `0700` directory owned by the deployment
user. It is never shared through a system `oll` account or group. Lock selection,
stale-socket recovery, and directory creation are defined in [node.md](node.md).

The wire contract is typed protobuf in `proto/oll/admin.proto`. The CLI parses
syntax once and converts it to a normalized domain request before opening the
channel:

```text
argv -> Clap types -> CliIntent -> prepared domain request -> protobuf Admin RPC
                                                          |
                                                          v
                                                typed response -> rendering
```

The client MUST NOT forward argv, option names, positional string arrays, or a
serialized Clap structure for the daemon to parse again. Doing so would create
two CLI parsers, make presentation syntax an internal RPC contract, weaken
field-level validation and redaction, and preserve invalid combinations beyond
the process boundary. Presentation-only options such as `status --json` remain
in the client and are not sent to the daemon.

This strong typing does not promise Admin API backward compatibility. The CLI
and daemon normally come from the same binary. Every request carries the exact
protocol descriptor fingerprint, and a client connecting to an older
still-running daemon receives a protocol-mismatch error with an instruction to
restart it. Schema changes are coordinated binary upgrades.

Admin connection establishment has a 10-second timeout independent of request
execution. Short local methods such as status, shutdown, log-filter changes,
document inspection, and operation listing carry a 10-second gRPC request
deadline. `ExportReplica` and `ImportReplica` deliberately carry no fixed client
request deadline: version 1 snapshots have no size limit, so a valid operation
may take much longer than ten seconds. They remain bounded by explicit client
termination and by the daemon's node-wide graceful-shutdown deadline; the
transport channel does not impose a hidden per-request timeout on them.
`PingPeer` is also short and uses the 10-second request deadline.
`SynchronizePeers` has no fixed client request deadline because connection
attempts and valid object/blob transfers may exceed it. Cancelling that Admin
wait does not implicitly tear down the daemon's persistent background sync
session.

The service grows with the required implementation order. The node stage owns
status, graceful shutdown, and typed live log-filter changes. Replica, sync, and
plugin RPCs are added only when their domain models have met the preceding
stage's completion criteria; the Admin API MUST NOT use stringly typed
placeholders for those future methods.

`GetStatus` returns the local node's complete `NodeIdentity`, not only its UUID-v4
`NodeId`, its configured listen address when present, plus each configured
connect target's state and optional remote identity learned through `SyncHello`.
It also includes authenticated inbound-only peers, whose status row has no
connect target, and reports connection direction explicitly.
Future sync and ping Admin requests use `NodeName` as their typed human-facing
selector after the daemon has learned that identity.

`SetLogFilter` receives a parsed target and typed level rather than a shell
directive. The CLI command `oll log set oll::sync=trace` owns that presentation
syntax, validates it, and sends the two typed fields. The change applies to the
running daemon only and resets at restart.

## Replica administration

The replica stage extends the typed Admin service with replica-specific methods;
it does not tunnel the `oll replica` argv through one generic RPC.

- `GetStatus` gains an explicit three-valued replica state: `uninitialized`
  with no `ReplicaId`, `initialized_empty` with an active `ReplicaId` and no
  visible entries, or `initialized_populated` with an active `ReplicaId` and
  one or more visible entries.
- `InspectReplicaDocument` resolves one managed text document and exposes its
  `CatalogNodeId`, `CatalogRevision`, `DocumentId`, `DocumentRevision`, path,
  media type, encoding, and byte size. It is not a generic directory or binary
  inspection RPC.
- `ListReplicaOperations` returns newest-first local high-level history for the
  selected document. Its records carry source, create/update/move/delete/replace
  kind, IDs, paths, time, and correlation ID, never Loro internals or content.
- `ExportReplica` writes a complete `.ollsnap` checkpoint to a validated local
  output path.
- `ImportReplica` validates a local `.ollsnap`, then initializes or replaces
  the one replica and marks its working-tree projection pending.

Both snapshot methods inherit the `correlation_id` in their
`AdminCallContext` through replica execution, structured snapshot lifecycle
events, asynchronous work, and snapshot-import operation records. The replica
layer must not replace that ID merely because it crosses from the Admin handler
into snapshot code.

The local CLI begins with native `PathBuf` inputs, so these Admin methods use a
separate replica-stage `NativePath` protobuf message containing raw Unix pathname
bytes. It is deliberately not `DocumentPath`: `DocumentPath` is a normalized,
absolute, UTF-8 address inside the logical replica namespace and remains the
document/plugin API type. Before serializing `NativePath`, the CLI resolves a
relative input against its startup working directory. The daemon then checks
absolute-path rules and, for document inspection/history, containment under
`replica_root` before converting it to `DocumentPath`. Snapshot source and
destination paths use the same native representation but have their own input
or output validation and are not required to be inside the working tree.
Containment alone is not sufficient for a managed-document request: every
relative segment must also meet the UTF-8 namespace rules in
[replica.md](replica.md).

The replica-stage wire shape is:

```proto
message NativePath {
  bytes unix_path = 1;
}
```

`unix_path` must be non-empty, absolute, and contain no NUL byte. It exists only
on the Unix Admin service; it is not a cross-platform pathname encoding.

`InspectReplicaDocument` and `ListReplicaOperations` return `INVALID_ARGUMENT`
when the contained path resolves to a directory or binary rather than a text
document. The broader catalog/document API may list those kinds, but this local
Admin surface keeps the already defined CLI commands document-scoped.

An uninitialized replica rejects inspect, operation-history, and export with
`FAILED_PRECONDITION`; import is allowed so it can initialize the slot. A path
outside the working tree for a document request is `INVALID_ARGUMENT`. The
replica protobuf update must use the method and message names fixed in this
section rather than introducing a generic command or entry-inspection RPC.

## Synchronization administration

The sync stage adds exactly two typed Admin methods:

```proto
rpc SynchronizePeers(SynchronizePeersRequest)
    returns (SynchronizePeersResponse);
rpc PingPeer(PingPeerRequest) returns (PingPeerResponse);
```

`SynchronizePeersRequest` carries `AdminCallContext`, an optional `NodeName`, and
`total_attempts`. The attempts value is greater than zero and includes the first
attempt. With no selector, the daemon captures the configured peer set at
request admission and runs one finite bidirectional inventory round for each.
With a selector, it resolves only a previously authenticated durable
`NodeIdentity`; an unknown name is `NOT_FOUND`. A deployment with no configured
peers and no selected learned peer is `FAILED_PRECONDITION`.

The response contains one result per captured peer, including identity when
known, attempts used, success/already-satisfied/failed outcome, transferred
object and blob counts, transferred bytes, and a typed error code plus redacted
message for a peer-local failure. Partial failure is therefore representable
without losing successful peer results. The CLI exits unsuccessfully if any
requested peer failed. A later edit after a round inventory was captured belongs
to the background manager's next round and does not keep this RPC open forever.

`PingPeerRequest` carries `AdminCallContext` and one required `NodeName`. It
resolves that learned identity, obtains or establishes an authenticated Noise
session, and measures a `SyncPing`/`SyncPong` exchange. The response returns the
confirmed `NodeIdentity` and round-trip milliseconds. It is not ICMP and success
proves sync-protocol/key/schema reachability at that instant, not replica
convergence.

`oll sync --log` remains a local file-view operation. `oll psk` remains a local
CSPRNG operation. Neither creates another Admin method. Correlation from the
Admin context propagates through connection attempts, finite rounds, transfers,
candidate activation, results, and logs. An inbound or background sync action
without Admin context creates its ID at the first daemon boundary.

## Errors

Admin method failures use gRPC status codes directly. They do not wrap every
response in `ProtocolError` or add a second error message to successful response
types. The request context is validated before method-specific work:

| Condition | gRPC status | Client-facing meaning |
| --- | --- | --- |
| exact schema fingerprint differs | `FAILED_PRECONDITION` | Restart the still-running daemon so it matches the CLI binary. |
| normalized request is malformed | `INVALID_ARGUMENT` | The client or caller constructed an invalid typed request. |
| managed-document path lies outside `replica_root` | `INVALID_ARGUMENT` | Use a document path inside the configured working tree. |
| replica operation requires an initialized replica | `FAILED_PRECONDITION` | Add a working-tree file and run oll, or import a snapshot first. |
| sync selector names no learned node | `NOT_FOUND` | Establish a configured authenticated connection or choose a name from status. |
| sync-all captures no configured peers | `FAILED_PRECONDITION` | Configure a connect target or select a learned peer. |
| daemon is stopping or the UDS cannot serve a request | `UNAVAILABLE` | Retry only after the daemon is ready again. |
| unexpected daemon failure | `INTERNAL` | Inspect the correlated daemon log event. |

The protocol-mismatch status message explicitly tells the user to restart the
running daemon. It is not a compatibility negotiation and carries no protobuf
error detail contract.

## Background startup

`oll start` uses a one-use readiness channel that is separate from the Admin
API:

1. The launcher verifies that no daemon owns the deployment's single-instance
   lock or answers on its Admin UDS.
2. It binds a loopback-only TCP listener to port `0`, allowing the kernel to
   choose an ephemeral port, and generates a 32-byte nonce with the operating
   system CSPRNG.
3. It resolves the deployment's config root against the launcher's startup
   working directory, then spawns the same executable as `oll run --config
   <absolute-config-root> --pingback <loopback-address>` in a detached process
   session with a piped stdin. `--pingback` is an internal, hidden `run` option.
   The nonce MUST NOT appear in argv or the environment.
4. The launcher writes exactly 32 nonce bytes to the child's stdin and closes
   the pipe.
5. The child acquires the single-instance lock, validates `node.json`, evaluates
   and validates configuration, initializes required log sinks, the node runtime,
   and the Admin UDS. Only when the daemon can serve administration requests
   does it read exactly 32 bytes from stdin, connect to the loopback pingback
   address, and write them back.
6. The launcher accepts connections for the 10-second startup deadline, compares
   the reply with the nonce in constant time, and reports success only on an
   exact match. Invalid or truncated replies do not consume the whole deadline.
7. On success the launcher closes its listener and exits. The detached daemon
   continues and is reparented by the operating system. On child exit, timeout,
   or handshake failure, `start` fails and MUST NOT report an uncertain success.
   On timeout or handshake failure, the launcher terminates and reaps the child
   it spawned before returning; it MUST NOT leave an unready daemon behind. Its
   Unix `SIGTERM`/two-second/`SIGKILL` escalation is defined in [node.md](node.md).

The loopback endpoint is not a reusable control port and closes after this one
startup. Randomness authenticates readiness without putting a secret in process
listings. It does not replace the deployment's single-instance lock or UDS file
permissions.

The implementation SHOULD spawn a new executable process directly rather than
calling raw `fork()` after a multithreaded Rust runtime has started. Detachment
also requires a new session and deliberate handling of inherited file
descriptors; merely allowing a child to become an orphan is not the complete
daemonization boundary.

## Shutdown

`oll stop` sends the typed Admin `Shutdown` request. The daemon acknowledges the
accepted request before beginning ordered graceful shutdown of listeners,
in-flight node work, and child processes. `accepted` is only acknowledgement,
not completion: the client waits for the shutdown condition and deadline in
[node.md](node.md). There is no second public daemon kill RPC. Process
supervisors may enforce termination outside the Admin API.

## Debugging

Debug Rust builds compile and register gRPC Server Reflection on the Admin UDS,
allowing local inspection with tools such as `grpcurl -unix`. Release builds
MUST compile the reflection service out; a runtime configuration switch is not
sufficient, and reflection MUST never be exposed on replication listeners.

At `TRACE`, the daemon records the RPC method, correlation ID, duration, result,
and an allowlisted, field-level-redacted parameter summary. It MUST NOT serialize
or log complete protobuf requests. Document content, plugin inputs, Lua values,
prompts, credentials, opaque payloads, and other fields prohibited by
`observability.md` remain secret at every log level.
