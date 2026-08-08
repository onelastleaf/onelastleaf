# Repository instructions

## Before changes

- Inspect the working tree and existing owners before editing.
- Before modifying code, read the relevant files under
  [`docs/`](docs/README.md); do not rely only on prior summaries.
- `docs/` defines architecture and behavior. `proto/oll/` defines wire
  contracts. Identify conflicts before implementation, and update the relevant
  documentation and protobuf first for approved behavior or protocol changes.

## Fixed architecture

- The only executable is the Clap-based `oll` daemon; there is no `olld`.
- One running `oll` daemon equals one node and exactly one replica.
- Never add in-process multi-replica mounting, switching, supervision, or
  routing.
- Connection roles (`connect` and `listen`) describe topology, not authority.
  Every node is an equal, writable CRDT replica.
- A replica contains one catalog `LoroDoc` and one `LoroDoc` per document.
- Paths are user-facing addresses; `DocumentId` is stable identity.
- Plugins are trusted independent processes and communicate with oll through the
  protobuf/gRPC boundary.
- Do not expose Loro-specific APIs, container IDs, frontiers, or version vectors
  through the document/plugin API. They are permitted only inside replication.

These constraints are final unless an explicit decision also updates the
architecture documentation.

## Change discipline

- Preserve unrelated user changes and keep edits scoped to the request.
- Fix incorrect behavior in its existing owner. Do not add parallel wrappers or
  adapters. Add production symbols only for a distinct responsibility or real
  reuse; remove replaced paths and audit new symbols for dead or redundant code.
- Prefer existing repository patterns and standard structured formats over ad
  hoc parsing.
- Every bug fix needs a regression test. Test in proportion to risk;
  distributed and persistent behavior needs failure, restart, and concurrency
  coverage.
- Cross-`LoroDoc` changes use the documented host transaction/recovery boundary.
  `.ollsnap` files are not Loro object snapshots.
- Observability is correctness: preserve structured lifecycle events,
  redaction, and correlation as specified in `docs/observability.md`.
- Never drop correlation context across RPC, sync, plugin, or Tokio task
  boundaries.
- Plugin stop begins with process-scoped `ShutdownRequest`; signals only enforce
  it. Job stop/timeout uses `CancelJobRequest` and never kills unrelated jobs.

## Commit discipline

- Follow the repository's existing Conventional Commit style and history.
- Never add an AI/model author, co-author, or `Co-authored-by` trailer.

## Required validation

For Rust changes, run focused tests plus at least `cargo fmt --check` and
`cargo check`.

For protobuf changes, run:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```

Also run `clang-format --dry-run --Werror` over all changed `.proto` files. Keep
the protocol documentation consistent with the generated descriptor.
