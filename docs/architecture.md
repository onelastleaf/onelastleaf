# Architecture

## Product boundary

onelastleaf (`oll`) is an offline-capable document library with peer-to-peer,
multi-writer CRDT replication and trusted process plugins. The connection
topology can be client/server shaped, but the replicated state has no authority
node.

The system has one executable named `oll`. It is a daemon with a Clap-based CLI
entry point. There is no separate `olld` binary.

## Cardinality invariant

The following relationship is final and MUST NOT be generalized:

```text
one running oll daemon = one node = one replica
```

Consequences:

- `oll` MUST NOT host, mount, switch between, or supervise multiple replicas.
- A node configuration has exactly one data directory and one `ReplicaId`.
- A path is unambiguous inside the daemon; document and plugin APIs do not need
  a `ReplicaId` routing field.
- Importing a snapshot never adds a second replica to a running daemon.
- Users who need multiple replicas MUST isolate multiple oll deployments using
  an external mechanism such as containers. oll itself does not manage them.
- Plugins, scheduler queues, jobs, logs, and Lua configuration all belong to the
  daemon's single replica.

The executable exposes administrative CLI commands and one daemon entry point,
`oll run`, but this does not make it a multi-instance manager. Process role is
selected only by the parsed subcommand; there is no global daemon/client mode.
Administrative clients use the typed gRPC-over-UDS boundary described in
[admin-api.md](admin-api.md).

## Terminology

### Node

A running `oll` daemon participating in replication. Its durable
`NodeIdentity` is the one-to-one pair of an opaque `NodeId` and a human-readable
`NodeName`. The name is declared by the node itself and is identical for every
peer; it is not a receiver-local connection label, a URL label, or an authority
role.

`NodeName` is a lowercase ASCII DNS label: 1 to 63 bytes, starting and ending
with an ASCII letter or digit, with hyphens allowed internally. The complete
`NodeIdentity` is created atomically by `oll init`. Neither member may be changed
or rebound independently in the first implementation: one `NodeId` has exactly
one `NodeName`, and one `NodeName` identifies exactly one `NodeId`. Reusing a
name for a replacement `NodeId` or renaming an existing `NodeId` requires an
explicit future identity-migration design.

There is no central name allocator. Two isolated nodes can therefore choose the
same name before they meet. Nodes persist the identity bindings they learn and
MUST reject a handshake if a known `NodeId` presents another name or a known
name presents another `NodeId`; they never resolve a collision by silently
renaming either endpoint. `NodeIdentity` is deployment state and is not copied
by replica snapshot import.

### Replica

The complete logical document tree owned by a node. It has one stable
`ReplicaId`. Nodes that synchronize the same logical library use the same
`ReplicaId` while retaining different `NodeId` values.

An oll replica is not one `LoroDoc`. It is a collection of CRDT objects:

```text
replica
├── catalog LoroDoc        # directory tree and document identities
└── document LoroDocs      # one LoroDoc per document
```

### Document

A stable `DocumentId`, one `LoroDoc`, and its catalog entry. A document path is
derived from its catalog position and is not its identity. Moving or renaming a
document does not replace its `DocumentId`.

### Deployment profile

`connect` and `listen` describe transport topology, not replication authority:

- a device commonly configures only `connect`;
- a publicly reachable node commonly configures `listen`;
- a relay-like node may configure both.

All profiles have the same replica read/write rights.

## Major components

```text
Clap CLI / daemon entry
          |
          v
       Node runtime
      /      |      \
 replica    sync    plugin supervisor
    |         |          |
 catalog +   gRPC bidi   plugin gRPC bidi
 documents   peers       + Lua callbacks
```

The node runtime owns lifecycle, configuration, the single replica, peer
connections, plugin processes, structured logs, and shutdown ordering.
The plugin supervisor shown above is the node runtime's internal owner of those
child processes. It is not an external process manager. It reconciles persisted
plugin desired state with process and protocol events supplied by the operating
system and plugin gRPC sessions.

All subcommands other than `run` are bounded client processes. Most communicate
with the running node through its Admin UDS. `init` is a local initialization
client and `start` is a local daemon launcher because neither operation can
presuppose an already-running daemon.

Structured logging is a cross-cutting runtime contract, not a final integration
task. The node initializes correlation context and user-owned log-directory sinks before
starting replica, sync, or plugin work. See
[observability.md](observability.md).

## Trust and consistency boundaries

- Plugins are fully trusted and may read or modify any document.
- Replica convergence is provided by Loro CRDTs.
- `Revision` preconditions protect application intent from stale plugin writes;
  they are not required for CRDT convergence.
- A CRDT commit and an external side effect cannot be atomic. oll provides no
  rollback, compensation, saga, or exactly-once promise for external systems.
- Runtime APIs do not expose Loro container IDs or library APIs to plugins.
- Replication may carry Loro-specific version vectors, frontiers, and encoded
  blobs because that boundary is internal to oll nodes.

## Compatibility policy

The runtime protobuf package has no `v1` namespace and provides no downgrade or
backward-compatibility negotiation. Peers and plugins require an exact schema
fingerprint match.

Persistent snapshot files are different: they can outlive a running binary and
therefore carry an explicit format version. Unsupported snapshot versions are
rejected rather than migrated implicitly.
