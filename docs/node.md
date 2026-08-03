# Node runtime

## Scope and platform

The node stage supplies the one long-running daemon shell. It owns the
deployment lock, `NodeIdentity`, Tokio runtime, logs, local Admin UDS, launch
handshake, and graceful shutdown. It does not create a `ReplicaId`, a catalog,
or a replica database. Those belong to the replica stage. It does establish the
complete configuration that tells the later replica stage where its working
tree and store will live.

The first node implementation is Unix-only. It supports Linux and Darwin and
uses Unix domain sockets, `flock`, `setsid`, inherited file descriptors, and
Unix signals. Windows is not a supported node deployment in this version; it
does not receive an incomplete named-pipe or process-lifecycle substitute.

One deployment has one config root, one running daemon, and one logical empty
replica slot. A second deployment uses a different config root and is a
separate process; the daemon never hosts multiple slots.

## Deployment layout

All paths below are user-owned. `node.json` is deliberately inside the config
root: it is durable user configuration, not an immutable host-owned secret.
The daemon validates it on every start but does not prevent the deployment user
from editing it.

```text
<config-root>/
  config.lua                 trusted executable configuration
  node.json                  durable NodeIdentity record
  replica.json               durable ReplicaId record; absent while uninitialized
  run/                       0700 runtime directory when used locally
    admin.sock               transient Admin UDS

<replica-root>/              user-editable working tree; no oll metadata here
<sqlite-store>/replica.sqlite3
                              oll-managed local store when driver = sqlite
PostgreSQL URL                external store location when driver = postgres
<log-dir>/                   user-owned JSON log files
```

The node-stage slot is only the configured `replica_root` directory. `oll init`
creates it when absent but does not assign a `ReplicaId`, write a catalog, or
create a replica database. It writes the required `replica_store` configuration
at the same time, but the replica stage opens that configured store. Existing
working-tree contents are never deleted or interpreted by the node stage.

`node.json` is strict JSON with this initial schema:

```json
{
  "format_version": 1,
  "node_id": "9ba4a1aa-4c7d-4b11-b902-3155cf8ca5f3",
  "node_name": "home-server"
}
```

`format_version` is the integer `1`. `node_id` must parse as a UUID version 4;
`oll init` writes its canonical lower-case hyphenated spelling, and the daemon
normalizes a valid user spelling before presenting it on the wire. `node_name`
uses the DNS-label syntax in [architecture.md](architecture.md). Unknown fields,
missing fields, non-string values, malformed JSON, and unsupported versions are
configuration errors.

`node_id` and `node_name` form one identity pair. The daemon does not rewrite a
user edit. A user who edits either one has deliberately created a different
pair; this is not an automatic identity migration. Remote nodes that already
learned the old binding will reject a contradictory pair during a later sync
handshake.

The daemon watches `node.json` and the replica-stage `replica.json` with
`notify`. An event is only a trigger: oll reopens the final path, strictly parses
the complete file, and compares it with the last accepted identity. A valid
atomic replacement or in-place edit is hot-loaded under one identity
coordinator. The coordinator pauses admission of replica commits, serializes
with bootstrap and snapshot identity transitions, advances a node-owned identity
epoch, and publishes the new identity before future local writes or handshakes.
Sync sessions added later bind that epoch and close themselves when it changes;
the node identity loader does not call a placeholder sync implementation. Historical
Loro operations and binary-version writer IDs are not rewritten.

A missing, transiently partial, or invalid runtime edit leaves the last accepted
identity active and emits a structured redacted error; a later watcher event or
periodic final-state check may accept a corrected file. Startup has no prior
valid identity to retain, so an invalid or missing `node.json` remains
`EX_CONFIG`. Active-replica startup also requires a valid `replica.json`; the
uninitialized state requires it to be absent except during a recognized durable
identity transition. Replica-specific SQL reconciliation is defined in
[replica-store.md](replica-store.md).

## Single-instance lock

The single-instance lock is acquired before `node.json` or `config.lua` is
loaded. This prevents an attempted second foreground `oll run` from executing
trusted Lua or creating runtime resources for a deployment already owned by a
daemon.

For a config root that exists, oll derives a deployment key from the SHA-256 of
the canonical Unix path bytes of that root. Canonicalization is used only to
make aliases of the same existing root select one lock; it does not rewrite the
configured root, persisted paths, or CLI path semantics. The lock file is
selected in this order:

1. `$XDG_RUNTIME_DIR/oll/<deployment-key>.lock` when `XDG_RUNTIME_DIR` names
   an existing usable directory;
2. `/run/user/<effective-uid>/oll/<deployment-key>.lock` when that directory
   already exists and is usable; oll never creates `/run/user/<uid>` itself;
3. `<config-root>/run/node.lock`.

For the first two choices oll may create only its own `oll` subdirectory. If an
`oll init` bootstrap must use the third choice for a not-yet-existing config
root, it creates the minimum `config-root/run` directory needed to take the
lock before writing configuration files.

oll opens its own lock file, takes a non-blocking exclusive Unix `flock`, and
keeps that file descriptor open until process exit. The filename may remain
after a crash; an existing file is not evidence of a running daemon, while a
held lock is. A held lock makes `run`, `start`, and `init` fail with
`EX_UNAVAILABLE` without evaluating Lua or changing deployment files.

Only after it owns the lock may a daemon create `<config-root>/run`, recover a
stale `admin.sock`, or bind the Admin UDS. It removes a stale path only after a
failed connection probe and only when that path is a Unix socket. A regular
file, directory, or other unexpected entry at the socket path is an error, not
something to overwrite. The runtime directory is owner-only (`0700`) so a
different local user cannot attach an Admin endpoint to the deployment.

## Initialization and recovery

`oll init <node-name>` is a local bootstrap operation. It first resolves its
config root, working-tree root, and log directory and takes the same deployment
lock. It checks for existing initialization material and obtains any required
confirmation before creating ordinary prerequisite directories. It then
generates one UUID-v4 `NodeIdentity` in memory, derives the default SQLite
store path from that exact `NodeId`, writes a complete initial `config.lua`, and
only after that succeeds writes `node.json` with the same identity. Each
replacement is written to a new sibling temporary file and atomically renamed
into its final path. oll does not follow a target file when replacing it. The
minimal lock-directory creation required by the third lock fallback is the one
bootstrap exception described above.

There is intentionally no transaction spanning `config.lua`, `node.json`, and
the configured directories. If a machine or process fails between the two file
writes, the deployment is incomplete rather than ambiguously initialized:

- `oll run` rejects a missing or invalid `node.json` with `EX_CONFIG` before
  starting node services;
- a valid `config.lua` alone does not prove initialization completed;
- rerunning `oll init` detects the existing initialization material and offers
  repair through the normal replacement confirmation.

If `<config-root>/config.lua`, `<config-root>/node.json`, or
`<config-root>/replica.json` already exists, `init` warns that it will replace
the first two, generate a new node identity pair, and remove the current replica
identity so the selected store slot is uninitialized. It does not delete the
working tree or old SQL store. It asks for `y`/`yes` or `n`/`no`; the default,
EOF, and unavailable input are negative and leave all three files unchanged. On
confirmation it writes the two new initialization files first and removes an
existing `replica.json` last; a crash before the final removal leaves an
incomplete deployment that a repeated `init` can repair, not a silently mixed
identity. No bypass flag exists in the first implementation. A running daemon
or concurrent `init` holds the lock and causes an immediate failure instead of a
prompt.

This initialization sequence does not claim a replica has been created. It
establishes only the configured empty working-tree/store slot and does not write
`replica.json`; replica initialization creates that file with the first complete
active replica.

## Startup

The foreground `oll run` sequence is:

1. capture the startup working directory and resolve only the config root;
2. derive and acquire the single-instance lock;
3. load and validate `node.json`;
4. evaluate and validate `config.lua`, then apply environment and CLI runtime
   overrides;
5. initialize the required log directory and sinks;
6. in the replica stage, open and recover the configured store and
   `replica.json` identity transition, complete any
   pending targeted or whole-tree projection before treating those paths as
   input, then register the recursive watcher, perform the initial scan, and
   reconcile events queued during that scan;
7. register final-state identity watches for `node.json` and `replica.json`;
8. in the sync stage, bind the configured sync listener and start outbound
   connection management;
9. recover or bind the Admin UDS, create the Tokio-owned node runtime, and mark
   the node ready;
10. when invoked by `oll start`, complete the one-use nonce pingback only after
   the Admin service can answer requests.

Configuration evaluation remains before every node service, but it is no
longer part of generic CLI preparation for `run`: the node runtime owns it so
the lock can precede trusted Lua execution. A Lua configuration that does not
return therefore holds its acquired lock but cannot create logs, an Admin
socket, a replica, or a network listener. The node-only implementation skips
step 6; the replica stage inserts it before the Admin service becomes ready so
no client can observe a half-recovered replica.

Each successful startup step is owned by a resource guard. A later synchronous
failure drops already acquired file descriptors, listeners, and temporary
runtime paths before returning an error. `Drop` is the right mechanism for
these local resources; it is not a substitute for explicitly awaiting and
cancelling asynchronous tasks during normal shutdown.

`oll start` performs a non-owning lock preflight before spawning. It releases
that temporary preflight lock before the child starts; the child acquiring the
real lock remains the authority. Two simultaneous launchers can therefore both
preflight, but exactly one child can become ready.

The launcher has a 10-second total readiness deadline, including lock
acquisition, identity validation, Lua evaluation, log setup, UDS binding, and
the nonce exchange. These are local operations and normally complete far below
that limit. A timeout or invalid pingback makes the launcher send `SIGTERM` to
its child, wait two seconds, then send `SIGKILL` if needed and reap the child.
It never reports an uncertain successful start or leaves its unready child
behind.

## Shutdown and signals

The node lifecycle is `starting`, `running`, then `stopping`. The first
accepted Admin `Shutdown` request atomically enters `stopping`; a later request
that reaches the service is idempotently accepted and does not begin a second
shutdown sequence.

The accepted response is written before shutdown begins. The ordered sequence
then is:

1. stop accepting new Admin connections and new node work;
2. close node listeners, including sync, stop accepting filesystem and identity
   events, send best-effort authenticated sync close frames, and notify owned
   tasks to stop;
3. wait for in-flight node work, including replica reconciliation, projection,
   sync sessions, and bootstrap tasks, through the 10-second
   graceful-shutdown deadline, then abort remaining local tasks;
4. write and flush the final structured lifecycle events;
5. remove the Admin socket and release the lock by dropping its descriptor;
6. exit the process.

The first accepted request or first termination signal records one absolute
deadline and one shutdown correlation ID. Admin draining and replica shutdown
use that same deadline concurrently; replica shutdown is not deferred until the
Admin server has finished. Stopping the replica first removes the operating
system watcher and wakes its event loop. Work from the watcher event already
being handled may drain until the deadline, but queued events do not begin new
reconciliations after shutdown starts. At the deadline the node aborts both the
remaining Admin work and replica-owned tasks, and Tokio blocking-task teardown
must not add a second unbounded wait before the socket and deployment lock are
released.

Each completed stage extends the owned-work set but must preserve this
externally visible ordering.
The daemon does not use an Admin "kill" method. If `oll stop` reaches its
deadline, it reports failure and does not escalate to an operating-system signal
on a daemon it did not spawn.

`oll stop` captures the deployment status, sends `Shutdown`, and treats
`accepted = true` as acknowledgement only. It waits up to 10 seconds for the
deployment lock to become acquirable and for the Admin socket to disappear or
the originally reported process to exit. Normal orderly shutdown removes the
socket before releasing the lock. A crash can leave a stale socket, but a free
lock and an exited original process still prove that the requested daemon is no
longer running; the next lock owner performs stale-socket recovery.

On Unix, the first `SIGINT` or `SIGTERM` follows the same ordered shutdown path
without an Admin acknowledgement. A second such signal terminates immediately;
`SIGKILL` is uncatchable and relies on kernel release of the lock descriptor.

`GetStatus` reports the complete `NodeIdentity`, lifecycle, start time, process
ID, configured listen address when present, and configured peer status. The
replica-stage extension distinguishes uninitialized, initialized-empty, and
initialized-populated state and includes `ReplicaId` only in the two initialized
states. The configured listen
value is not a claim that the later sync listener is already bound during the
node-only stage.

Dynamic log filtering is part of node lifecycle: `oll log set
<target>=<level>` changes the live daemon through the typed Admin API and is
defined in [observability.md](observability.md). It is not persisted in
`config.lua`, `node.json`, or `replica.json`.
