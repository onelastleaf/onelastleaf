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
| snapshot inspect/verify, log viewing, and `plugin validate` | local file client | Validates or reads one local file and exits. |
| remaining operational subcommands | admin client | Opens the configured Admin API, makes a bounded request, renders the result, and exits. |

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

The service grows with the required implementation order. The node stage owns
status, graceful shutdown, and typed live log-filter changes. Replica, sync, and
plugin RPCs are added only when their domain models have met the preceding
stage's completion criteria; the Admin API MUST NOT use stringly typed
placeholders for those future methods.

`GetStatus` returns the local node's complete `NodeIdentity`, not only its UUID-v4
`NodeId`, its configured listen address when present, plus each configured
connect URL's state and optional remote identity learned through `SyncHello`.
Future sync and ping Admin requests use `NodeName` as their typed human-facing
selector after the daemon has learned that identity.

`SetLogFilter` receives a parsed target and typed level rather than a shell
directive. The CLI command `oll log set oll::sync=trace` owns that presentation
syntax, validates it, and sends the two typed fields. The change applies to the
running daemon only and resets at restart.

## Errors

Admin method failures use gRPC status codes directly. They do not wrap every
response in `ProtocolError` or add a second error message to successful response
types. The request context is validated before method-specific work:

| Condition | gRPC status | Client-facing meaning |
| --- | --- | --- |
| exact schema fingerprint differs | `FAILED_PRECONDITION` | Restart the still-running daemon so it matches the CLI binary. |
| normalized request is malformed | `INVALID_ARGUMENT` | The client or caller constructed an invalid typed request. |
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
