# Replica snapshot format

## Purpose

A Loro snapshot serializes one `LoroDoc`. An oll replica contains a catalog
`LoroDoc` plus many document `LoroDoc` objects, so replica export/import requires
an outer container.

The oll replica snapshot format is a structurally constrained tar archive
compressed as one zstd frame. The conventional extension is `.ollsnap`.

```text
<name>.ollsnap
└── zstd frame
    └── POSIX tar archive
        ├── manifest.json
        ├── catalog.loro
        └── documents/<document-id>.loro
```

This is a complete replica checkpoint, not a sync delta and not a backup of the
daemon's configuration.

## Archive rules

An exporter MUST:

- produce a POSIX tar archive inside a zstd frame;
- enable the zstd frame checksum;
- place `manifest.json` first and `catalog.loro` second;
- place document entries afterward in lexicographic `DocumentId` order;
- use regular files only, with no links, devices, sparse files, or absolute
  paths;
- normalize tar owner/group IDs, names, modes, and modification times;
- use IDs rather than user-controlled document paths as archive entry names.

Compression level is not part of the format. Two valid exports may have
different compressed bytes while containing the same logical checkpoint.

An importer MUST reject duplicate entries, unknown required entry types, path
traversal, links, size overflow, checksum mismatch, and trailing undeclared
payloads.

The zstd checksum and manifest hashes detect accidental corruption. They do not
authenticate who created a snapshot: version 1 has no signature or trust-store
mechanism.

## Manifest

`manifest.json` is UTF-8 JSON with this initial logical schema:

```json
{
  "format": "onelastleaf-replica-snapshot",
  "format_version": 1,
  "snapshot_id": "opaque-unique-id",
  "replica_id": "logical-replica-id",
  "created_at": "2026-07-25T00:00:00Z",
  "loro_encoding_fingerprint": "hex-encoded-sha256",
  "catalog": {
    "entry": "catalog.loro",
    "size_bytes": 1234,
    "sha256": "hex-encoded-sha256"
  },
  "documents": [
    {
      "document_id": "stable-document-id",
      "state": "live",
      "entry": "documents/stable-document-id.loro",
      "size_bytes": 5678,
      "sha256": "hex-encoded-sha256"
    }
  ]
}
```

Document `state` is `live` or `tombstoned`. The list includes every retained
document object required to reconstruct the replica, not only visible files.

`format_version` versions the persistent container, not the runtime plugin API.
The first implementation accepts only version `1` and rejects every other
value. It does not guess or silently migrate.

The Loro fingerprint must match the importer's supported snapshot encoding.
Each `.loro` entry is produced using Loro's full snapshot export mode.

## Consistent export

Loro does not provide a transaction spanning multiple `LoroDoc` objects.
Export therefore takes a replica-wide consistency barrier:

1. stop admitting local commits and remote object imports;
2. finish any commit already inside the write coordinator;
3. capture the catalog and the complete retained-document set;
4. export every Loro snapshot into a private staging directory while calculating
   size and SHA-256;
5. build the manifest and archive;
6. release the barrier;
7. atomically publish the completed `.ollsnap` file.

A failed export removes staging data and never exposes a partial destination
file. The first implementation favors correctness over minimizing barrier time.

## Import validation

Import is staged before any replica mutation:

1. stream-decompress zstd with explicit compressed and uncompressed limits;
2. validate tar entry types and paths;
3. parse and validate the manifest using a structured JSON parser;
4. verify the exact declared entry set, byte sizes, and SHA-256 hashes;
5. validate the format and Loro encoding fingerprints;
6. open every Loro snapshot in staging to prove it is decodable;
7. build a complete candidate replica store without mutating the active store;
8. only then acquire the replica write coordinator and atomically install the
   candidate.

Import must not load the complete archive into memory.

## Import modes

Because one daemon has exactly one replica, import never mounts an additional
replica.

### Initialize

If the node's replica slot is empty, import initializes it from the snapshot.
It preserves `ReplicaId` and `DocumentId` values but the deployment retains or
creates its own `NodeIdentity` and uses fresh peer identity for future Loro
changes.

### Replace

If a replica already exists, the snapshot `ReplicaId` MUST equal the current
`ReplicaId`. Import is a destructive restore, not a CRDT merge: documents and
catalog state that exist only in the active replica are removed. The CLI must
separately confirm that the user has exported the active replica to a backup
snapshot and that the user accepts replacement before it sends the import
request.

Replacement requires the daemon's normal mutation, sync, and plugin activity to
be stopped. It installs the fully validated staged store by atomic rename and
keeps a recoverable previous store until replacement succeeds. A failed import
leaves the active replica byte-for-byte untouched.

A snapshot with a different `ReplicaId` is rejected. oll does not convert that
case into a second managed replica or silently fork identities.

## Excluded state

A replica snapshot MUST NOT contain:

- `NodeIdentity` or Loro peer identity used for future local operations;
- `connect`/`listen` configuration;
- plugin executables, source trees, or package-manager state;
- Lua configuration, secrets, or credentials;
- scheduler queues, running jobs, logs, caches, or temporary artifacts.

These belong to a node deployment, not to the logical document replica.
