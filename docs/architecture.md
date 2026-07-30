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
- A completed deployment has exactly one user-editable working tree, one
  configured replica store, and one `ReplicaId`. The node stage reserves that
  one slot; the replica stage creates its `ReplicaId` and store contents.
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
`NodeIdentity` is the one-to-one pair of a UUID-v4 `NodeId` and a human-readable
`NodeName`. The ID carries no routing or authority semantics. The name is
declared by the node itself and is identical for every peer; it is not a
receiver-local connection label, a URL label, or an authority role.

`NodeName` is a lowercase ASCII DNS label: 1 to 63 bytes, starting and ending
with an ASCII letter or digit, with hyphens allowed internally. `NodeId` is a
UUID v4. `oll init` generates the initial pair in the user-owned
`<config-root>/node.json` record described in [node.md](node.md). The record is
validated, not treated as an immutable host-owned secret: its deployment user
may edit it, and a valid edit takes effect at the next daemon start.

At every protocol boundary one `NodeId` has exactly one `NodeName`, and one
`NodeName` identifies exactly one `NodeId`. Replacing either field in
`node.json` deliberately creates a new local pairing; oll does not provide a
separate rename or migration workflow. A user who does this is responsible for
the consequences when that new pair meets peers that already recorded the old
binding.

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

An oll replica is not one `LoroDoc`. Its user-editable working tree is separate
from the oll-managed replica store. The store contains a collection of CRDT
objects and content-addressed binary blobs:

```text
replica
├── catalog LoroDoc        # directory tree, text/binary identities, metadata
├── document LoroDocs      # one LoroDoc per text document
└── binary blobs           # LWW bytes, keyed by SHA-256
```

The working tree contains only ordinary user-editable directories and files.
It never contains hidden catalog, Loro, or journal files. The configured SQL
store is the durable recovery authority; its layout and reconciliation rules
are in [replica-store.md](replica-store.md).

### Document

A supported text file has a stable `DocumentId`, one `LoroDoc`, and a catalog
entry. Its path is derived from catalog position and is not its identity.
Moving or renaming a document does not replace its `DocumentId` when oll
observes a reliable live rename. A binary file instead has a `BinaryId`, a
catalog entry, and LWW blob versions; it has no Loro document.

### Connection topology

`connect` and `listen` describe transport topology, not replication authority:

- a device commonly configures only `connect`;
- a publicly reachable node commonly configures `listen`;
- a relay-like node may configure both.

Every topology has the same replica read/write rights.

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
 docs/blobs  peers       + Lua callbacks
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
- `CatalogRevision` and `DocumentRevision` preconditions protect application
  intent from stale path/metadata and text/CRDT writes respectively; they are
  not required for CRDT convergence.
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
rejected rather than migrated implicitly. Replica and sync compatibility use
the exact protobuf descriptor fingerprint; Loro payload compatibility is proven
by actual decode/import rather than a second Loro encoding fingerprint.
