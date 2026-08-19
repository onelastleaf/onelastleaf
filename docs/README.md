# onelastleaf design documentation

`docs/` records implementation decisions that must be understood before code is
written. Protobuf wire details remain in [`proto/`](../proto/README.md); these
documents describe the larger runtime and storage model around those messages.

## Documents

- [Architecture](architecture.md): system boundaries, terminology, and the
  one-binary/one-node/one-replica invariant.
- [C4 architecture views](c4-architecture/): current system, container, bounded
  client, daemon, replica, sync, and plugin ownership diagrams.
- [Command-line interface](cli.md): commands, arguments, defaults, conflicts,
  environment precedence, and exit behavior.
- [Configuration runtime](configuration.md): the executable `config.lua`
  contract, raw sync network-key input, typed result schema, path precedence,
  and embedded LuaJIT build.
- [Node runtime](node.md): user-owned node/replica identities, deployment layout,
  single-instance locking, startup, shutdown, and Unix platform boundary.
- [Local administration API](admin-api.md): subcommand-selected process roles,
  the typed gRPC-over-UDS boundary, background startup, and local debugging.
- [Replica model](replica.md): the document tree, catalog, per-document
  `LoroDoc`, binary entries, revisions, and local commit semantics.
- [Replica store and working tree](replica-store.md): the user-editable file
  tree, SQL-backed CRDT store, filesystem reconciliation, and recovery rules.
- [Snapshot format](snapshot-format.md): the `.ollsnap` tar+zstd container and
  export/import behavior for catalog, text documents, and binary blobs.
- [Synchronization](synchronization.md): peer-to-peer CRDT replication over
  TCP + Noise PSK, finite object-level rounds, and atomic bootstrap.
- [Observability](observability.md): structured JSON logs, aggregation,
  correlation propagation, file routing, and retention.
- [Plugin system](plugin-system.md): process lifecycle, oll-hosted bidi gRPC,
  jobs, configuration callbacks, artifacts, and logs.
- [Plugin SDKs](plugin-sdk.md): official language runtimes, package identities,
  conformance, and local project generation.
- [Plugin storage](plugin-storage.md): deployment-local package generations,
  SQL authority, recovery, desired state, jobs, and removal.
- [Plugin packaging](plugin-packaging.md): system Git, `oll.toml`, typed masks,
  `oll-release.json`, `plugins.lua`, source recipes, and release artifacts.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. A section explicitly
marked `Open` is not an implementation decision. Everything else is the current
design contract and should be changed in documentation and protocol definitions
before incompatible code is written.
