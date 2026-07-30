# Replica snapshot format

## Purpose

A Loro snapshot serializes one LoroDoc. An oll replica additionally contains a
catalog, multiple text-document LoroDocs, and raw binary blobs, so replica
export/import requires an outer container.

The oll replica snapshot format is a structurally constrained POSIX tar archive
inside one zstd frame. The conventional extension is `.ollsnap`.

```text
<name>.ollsnap
└── zstd frame
    └── POSIX tar archive
        ├── manifest.json
        ├── catalog.loro
        ├── documents/<document-id>.loro
        └── blobs/<sha256>
```

This is a complete logical-replica checkpoint, not a sync delta and not a
backup of node configuration. The user-editable working tree is reconstructed
from catalog, document, and blob state; it is not archived as a second,
independent source of truth.

## Archive rules

An exporter MUST:

- produce one POSIX tar archive inside one zstd frame with its checksum enabled;
- place `manifest.json` first and `catalog.loro` second;
- place document entries afterward in lexicographic canonical `DocumentId`
  order and blob entries in lexicographic lower-case SHA-256 order;
- use regular files only, with no links, devices, sparse files, or absolute
  paths;
- normalize tar owner/group IDs, names, modes, and modification times;
- use UUIDs and hashes rather than user-controlled working-tree paths as
  archive entry names.

Compression level is not part of the format. Two valid exports can have
different compressed bytes while containing the same logical checkpoint.

An importer MUST reject duplicate entries, unknown or invalid entry types, path
traversal, links, checksum mismatch, malformed hashes, size overflow, and
trailing undeclared payloads. It streams decompression, tar validation, and
blob handling; it MUST NOT load the complete archive into memory.

Version 1 deliberately has no configured compressed-size or uncompressed-size
limit. The user who imports a snapshot is responsible for the available disk,
memory, and CPU resources. Resource exhaustion remains an import failure, not
a reason to accept a partial archive.

The zstd checksum and manifest hashes detect accidental corruption. They do not
authenticate who created a snapshot: version 1 has no signature or trust-store
mechanism.

## Manifest

`manifest.json` is strict UTF-8 JSON with this version-1 schema. Unknown,
duplicate, missing, or wrongly typed fields are rejected.

```json
{
  "format": "onelastleaf-replica-snapshot",
  "format_version": 1,
  "snapshot_id": "9ba4a1aa-4c7d-4b11-b902-3155cf8ca5f3",
  "replica_id": "44d62c47-0d82-42f0-a767-e3d6d5e75858",
  "created_at": "2026-07-30T00:00:00Z",
  "catalog": {
    "entry": "catalog.loro",
    "size_bytes": 1234,
    "sha256": "hex-encoded-sha256"
  },
  "documents": [
    {
      "document_id": "60c8b0de-1d43-4f48-9a9c-13b7d19af3b4",
      "state": "live",
      "entry": "documents/60c8b0de-1d43-4f48-9a9c-13b7d19af3b4.loro",
      "size_bytes": 5678,
      "sha256": "hex-encoded-sha256"
    }
  ],
  "blobs": [
    {
      "entry": "blobs/hex-encoded-sha256",
      "size_bytes": 1048576,
      "sha256": "hex-encoded-sha256"
    }
  ]
}
```

`snapshot_id`, `replica_id`, and each `document_id` are canonical UUID-v4
strings. `created_at` is an RFC 3339 UTC timestamp. SHA-256 values are lower
case 64-character hex strings. A blob's archive entry basename MUST equal its
declared hash.

Document `state` is `live` or `tombstoned`. The document list includes every
retained document object required to reconstruct the replica, not only visible
files. The blob list includes every blob hash referenced by retained binary
version records in the catalog. The catalog itself maps `BinaryId`, path,
media type, LWW stamp, and version history to those hashes.

`format_version` versions the persistent container, not the runtime plugin API.
The first implementation accepts only `1` and rejects every other value. It
does not guess or silently migrate. There is no Loro encoding fingerprint:
each `.loro` entry uses Loro full-snapshot export, and actual Loro decode/import
is the compatibility check.

## Consistent export

Loro does not provide a transaction spanning multiple LoroDocs. Export takes a
replica-wide consistency barrier:

1. stop admitting local commits, working-tree imports, and remote object
   imports;
2. finish a commit already inside the write coordinator;
3. capture catalog, retained document objects, and retained binary-version
   records;
4. export each Loro full snapshot and stream each required blob from the
   replica store into a private staging directory while calculating size and
   SHA-256;
5. build the manifest and archive;
6. release the barrier;
7. atomically publish the completed `.ollsnap` output.

A failed export removes staging data and never exposes a partial destination
file. The first implementation favors correctness over minimizing barrier time.
It also never overwrites an existing destination because the CLI has no
`--force` option. The exporter creates a private temporary file in the
destination's directory, finishes and flushes it, then publishes it with an
atomic no-replace operation. An existing path at the initial check or a path
created by a racing process causes `ALREADY_EXISTS`; the existing path is not
opened, truncated, renamed, or removed.

## Local inspection and verification

`oll replica snapshot inspect <snapshot>` validates the zstd/tar prefix and the
strict manifest, then prints only manifest metadata. It does not claim that
later payload hashes or Loro objects have been verified. Human-readable output
has this stable field order, one `key: value` per line:

```text
format: onelastleaf-replica-snapshot
format_version: 1
snapshot_id: <uuid-v4>
replica_id: <uuid-v4>
created_at: <rfc3339-utc>
live_documents: <count>
tombstoned_documents: <count>
blobs: <count>
catalog_bytes: <count>
document_bytes: <count>
blob_bytes: <count>
```

The byte totals are the manifest-declared uncompressed sizes; they are not the
compressed file size. `--json` emits exactly the same information as one JSON
object with keys `format`, `format_version`, `snapshot_id`, `replica_id`,
`created_at`, `live_documents`, `tombstoned_documents`, `blobs`,
`catalog_bytes`, `document_bytes`, and `blob_bytes`. Counts and byte totals are
JSON integers. Unknown fields are not added without changing this output
contract.

`oll replica snapshot verify <snapshot>` performs the complete streaming
archive validation: it checks the exact declared entry set and order, entry
types and paths, sizes, every SHA-256, catalog-to-document/blob references, and
decodability plus the fixed schema of every Loro snapshot. On success it writes
exactly `verified snapshot <snapshot-id>` followed by a newline. On failure it
writes no success line, returns a nonzero status, and reports which validation
class failed without dumping archive content. Both commands are local file
clients and do not read the current deployment configuration or replica.

## Import validation

Import is staged before active-replica mutation:

1. stream-decompress and validate tar entry types and paths;
2. parse and strictly validate the manifest using a structured JSON parser;
3. verify the exact declared entry set, byte sizes, and SHA-256 hashes;
4. open every Loro snapshot in staging to prove it is decodable;
5. verify that catalog-referenced retained binary hashes are present in staged
   blobs and that no blob is undeclared;
6. build a complete candidate store without mutating the active store;
7. only then acquire the replica write coordinator and atomically commit the
   candidate catalog, documents, blobs, `ReplicaId`, fresh local `LoroPeerId`,
   and `projection_pending` marker.

The candidate is an inactive SQL generation. The final commit changes the
store's `active_generation` pointer and sets `projection_pending` in one
transaction, as specified in [replica-store.md](replica-store.md). A crash
before that pointer switch leaves the old generation active. A crash after it
makes the imported generation authoritative and forces projection recovery;
startup never guesses between generations from working-tree contents.

## Import modes

Because one daemon has exactly one replica, import never mounts an additional
replica.

### Initialize

If the store is uninitialized, import initializes it from the snapshot. It
preserves snapshot `ReplicaId`, `CatalogNodeId`, `DocumentId`, and `BinaryId`
values. The deployment keeps its own `NodeIdentity` and creates a fresh local
`LoroPeerId` for future Loro operations.

### Replace

If a replica already exists, import is a destructive restore, not a CRDT merge.
Immediately before sending the Admin request, the CLI separately asks whether
the user exported the current replica to a backup and whether the user accepts
replacement. Negative answers, EOF, or unavailable interactive input cancel
without sending the request.

Replacement MAY use a snapshot with a different `ReplicaId`. After both
confirmations, oll discards the old active replica state and adopts the
snapshot's `ReplicaId`; it never retains the old ID while loading unrelated
logical content. This still does not add a second replica or silently mount a
second store.

The candidate-store transaction sets `projection_pending` before the new
working tree is materialized. The projection removes managed paths absent from
the imported catalog and writes the imported visible documents and binary blobs.
If oll stops at any point after the store transaction, its next startup skips
old-tree import and finishes that reconstruction before enabling the watcher.
A failed validation or candidate build leaves the active store and working tree
untouched.

## Excluded state

A replica snapshot MUST NOT contain:

- `NodeIdentity` or a separate active `LoroPeerId` to use for future operations;
- `connect`/`listen` configuration;
- the SQL backend's physical layout, connection URL, credentials, locks, or
  operation-history records;
- plugin executables, source trees, or package-manager state;
- Lua configuration, secrets, credentials, logs, caches, or temporary
  artifacts.

These belong to a node deployment rather than the logical replica.
Historical peer IDs already encoded in catalog/document Loro operations and
version vectors necessarily remain part of those object snapshots. Import uses
them only as history and chooses a different active local peer ID as described
in [replica-store.md](replica-store.md).

## Required tests

Snapshot tests cover deterministic entry ordering, empty and populated replica
round trips, retained tombstoned documents, deduplicated blobs, destination
no-replace races, and both same- and different-`ReplicaId` replacement. Negative
fixtures cover malformed and duplicate manifest fields, undeclared or duplicate
entries, wrong order/type/size/hash, zstd corruption, trailing data, absolute or
traversing paths, links and special files, missing referenced objects, extra
blobs, invalid catalog/document schemas, and undecodable Loro bytes. Streaming
tests use inputs larger than the implementation buffers to prove that inspect,
verify, export, and import do not require the complete archive or blob in memory.
