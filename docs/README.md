# onelastleaf design documentation

`docs/` records implementation decisions that must be understood before code is
written. Protobuf wire details remain in [`proto/`](../proto/README.md); these
documents describe the larger runtime and storage model around those messages.

## Documents

- [Architecture](architecture.md): system boundaries, terminology, and the
  one-binary/one-node/one-replica invariant.
- [Implementation order](implementation-order.md): the required CLI -> node ->
  replica -> sync -> plugin-system sequence.
- [Command-line interface](cli.md): commands, arguments, defaults, conflicts,
  environment precedence, and exit behavior.
- [Configuration runtime](configuration.md): the executable `config.lua`
  contract, typed result schema, path precedence, and embedded LuaJIT build.
- [Local administration API](admin-api.md): subcommand-selected process roles,
  the typed gRPC-over-UDS boundary, background startup, and local debugging.
- [Replica model](replica.md): the document tree, catalog, per-document
  `LoroDoc`, revisions, and local commit semantics.
- [Snapshot format](snapshot-format.md): the `.ollsnap` tar+zstd container and
  export/import behavior.
- [Synchronization](synchronization.md): peer-to-peer CRDT replication and its
  object-level protocol.
- [Observability](observability.md): structured JSON logs, aggregation,
  correlation propagation, file routing, and retention.
- [Plugin system](plugin-system.md): process lifecycle, bidi gRPC, jobs,
  configuration callbacks, scheduling, logs, and package installation.
- [Plugin packaging](plugin-packaging.md): Git remotes, `oll.toml`, `oll.json`,
  `plugins.lua`, source recipes, release artifacts, and installation errors.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. A section explicitly
marked `Open` is not an implementation decision. Everything else is the current
design contract and should be changed in documentation and protocol definitions
before incompatible code is written.
