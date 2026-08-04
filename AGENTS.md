# Repository instructions

## Mandatory preflight

Before planning, writing, or modifying any implementation code, read
[`docs/implementation-order.md`](docs/implementation-order.md) in full. Do not
rely on a summary or a previous conversation. Implementation must follow its
required order:

```text
CLI (Clap) -> node -> replica -> sync -> plugin system
```

Do not implement a later stage before the earlier stage's documented completion
criteria are satisfied. An earlier stage must not depend on a placeholder from a
later stage.

The implementation order is a development constraint only. Never represent it
with runtime enums, modes, configuration, protocol fields, persisted state, or
user-facing stage-specific errors.

Read the other relevant files under [`docs/`](docs/README.md) before changing a
component's behavior, storage model, protocol, or lifecycle.

## Fixed architecture

- The project has one executable named `oll`. There is no `olld`.
- `oll` is a daemon with a Clap-based CLI entry point.
- One running `oll` daemon equals one node and exactly one replica.
- Never add in-process multi-replica mounting, switching, supervision, or
  routing.
- Connection roles (`connect` and `listen`) describe topology, not authority.
  Every node is an equal, writable CRDT replica.
- A replica contains one catalog `LoroDoc` and one `LoroDoc` per document.
- Paths are user-facing addresses. `DocumentId` is the stable document identity.
- Plugins are trusted independent processes and communicate with oll through the
  protobuf/gRPC boundary.

These constraints are final. Do not generalize them without an explicit user
decision that also updates the architecture documentation.

## Sources of truth

- `docs/` defines architecture, implementation order, storage behavior,
  snapshot semantics, synchronization, and plugin lifecycle.
- `proto/oll/` defines wire messages and gRPC services.
- When a requested change conflicts with these documents, identify the conflict
  before implementing it.
- Update the relevant design document and protobuf contract before implementing
  an approved architectural or wire-level change.
- Do not expose Loro-specific APIs, container IDs, frontiers, or version vectors
  through the document/plugin API. They are permitted only inside replication.

## Change discipline

- Inspect the existing code and working tree before editing.
- Preserve unrelated user changes and keep edits scoped to the active stage.
- Minimize new semantics. When an existing function, type, or module owns the
  incorrect behavior, fix that owner directly instead of adding a parallel
  path, wrapper, adapter, or compensating abstraction around it.
- Add a new production function, type, module, or state concept only when it
  has a distinct semantic responsibility or meaningful reuse. Be especially
  cautious with single-call helpers and glue code: keep the logic in its
  existing owner unless extraction isolates a genuinely non-trivial algorithm,
  invariant, or error boundary. Remove the obsolete path when a replacement is
  introduced, and audit newly added production symbols for dead or redundant
  code before considering the change complete.
- Prefer existing repository patterns and standard structured formats over ad
  hoc parsing.
- Treat tests as the executable guarantee of program correctness. Every bug fix
  needs a regression test that fails for the original defect and passes after
  the fix; do not rely on code review or reasoning alone. Add tests in
  proportion to the behavior being introduced. Distributed and persistence
  behavior requires failure, restart, and concurrency coverage.
- Do not claim cross-`LoroDoc` operations are one Loro transaction. Follow the
  documented host-level commit and recovery boundary.
- Do not treat oll replica snapshots (`.ollsnap`) as Loro object snapshots; they
  have different formats and purposes.
- Treat observability as part of correctness. New operations need structured
  start/success/failure events, redaction, and correlation propagation according
  to `docs/observability.md`.
- Never drop correlation context across RPC, sync, plugin, or Tokio task
  boundaries.
- Plugin stop is process-scoped and begins with `ShutdownRequest`; signals only
  enforce that request when the process does not exit. Job stop and job timeout
  use the job-scoped `CancelJobRequest` and must not terminate unrelated jobs.

## Commit discipline

- Follow the repository's existing Conventional Commit style and history.
- Never add an AI/model author, co-author, or `Co-authored-by` trailer.

## Required validation

For Rust changes, run the applicable formatter, checks, and focused tests. At a
minimum, use `cargo fmt --check` and `cargo check` once the current stage has a
compilable Rust implementation.

For protobuf changes, run:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```

Also run `clang-format --dry-run --Werror` over all changed `.proto` files. Keep
the protocol documentation consistent with the generated descriptor.
