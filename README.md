<div align="center">

  ![logo](https://preview.github.sov710.org/onelastleaf/onelastleaf-logo.svg?)

  # onelastleaf

  [![License](https://img.shields.io/github/license/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=bb9af7)](LICENSE)
  [![Last Commit](https://img.shields.io/github/last-commit/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=7aa2f7)](https://github.com/onelastleaf/onelastleaf/commits/main)
  [![Stars](https://img.shields.io/github/stars/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=7aa2f7&logo=github&logoColor=white)](https://github.com/onelastleaf/onelastleaf/stargazers)
</div>

onelastleaf is a self-hosted, CRDT-powered document library for syncing
documents across devices and extending document workflows with plugins.

## Contents

- [Why the name *onelastleaf*?](#why-the-name-onelastleaf)
- [What is onelastleaf?](#what-is-onelastleaf)
- [Why build another document library?](#why-build-another-document-library)
  - [Why not just use Syncthing?](#why-not-just-use-syncthing)
  - [Why a CRDT?](#why-a-crdt)
  - [Why plugins?](#why-plugins)
- [Quick start](#quick-start)
- [Configuration](#configuration)
  - [`config.lua`](#configlua)
  - [`node.json`](#nodejson)
  - [`replica.json`](#replicajson)
- [Installation](#installation)
- [Project layout](#project-layout)
- [License](#license)

## Why the name *onelastleaf*?

The first inspiration was [*One Last Kiss*](https://music.apple.com/us/album/one-last-kiss/1542953969),
the theme song from the final [*Evangelion*](https://www.evangelion.jp/) film.

The most recognizable organization in *Evangelion* is NERV, whose emblem
features a fig leaf. The interpretation behind the name is summarized well
here:

> I always assumed it was a fig leaf. When Adam & Eve ate of the fruit of
> knowledge and realised they were naked, they made clothing out of fig leaves.
> Biblically it was the first thing human beings made for themselves. So I think
> the logo represents Nerv holding forbidden knowledge and creating something
> out of it, namely the Evas. --- Hideous-Kojima,
> [Reddit](https://www.reddit.com/r/evangelion/comments/1lk9phm/what_is_the_nerv_logo_supposed_to_be/)

Calling the project *One Last Kiss* would have made the reference a little too
obvious, so *kiss* became *leaf*. A leaf is part of a book, a fig leaf is part of
the NERV imagery, and both point back to the same image: knowledge made tangible
and preserved.

## What is onelastleaf?

onelastleaf has three central pieces:

- **Multi-device, peer-to-peer synchronization.** Keep the same document
  library on a desktop, laptop, phone, or another machine without making one
  device the permanent authority.
- **CRDT-based conflict resolution.** Concurrent edits converge without
  reducing every text conflict to “choose this entire file or that one.” Text
  documents are merged through [Loro](https://loro.dev/); binary files are
  synchronized but do not receive text-level CRDT merging. The working tree
  remains a directory of ordinary files that can be edited with ordinary tools.
- **A plugin system.** Run document-focused workflows outside the core:
  generate study material, transform notes, call external tools, or add new
  interfaces over the same library.

The project is self-hosted. There is no onelastleaf SaaS service today. A hosted
version would need a real end-to-end encryption design first. The current
protocol encrypts and authenticates peer connections with a shared network key.
It is designed for trusted members of a sync network, not to hide data from an
untrusted hosted service or from a malicious member with the key.

## Why build another document library?

Human memory is unreliable. Knowledge lasts longer when it is written down and
revisited. The simplest way to do that is to write notes—or, in more
programmer-friendly language, to write documentation.

At first, that sounds like a solved problem: open any editor and keep a folder
of Markdown files. That works until the folder needs to follow its user. A
document library should remain readable and editable from a phone, a lightweight
laptop, or whichever machine happens to be nearby—not only from one desktop.

So synchronization is not an extra feature; it is the first requirement.

### Why not just use [Syncthing](https://syncthing.net/)?

File synchronization and conflict resolution are different problems.
Syncthing and similar tools are excellent at moving files between machines, but
their unit of synchronization is fundamentally the file. When two devices edit
the same document independently, a file-level synchronizer cannot generally
combine both sets of textual changes into one result without producing conflict
copies or choosing one version.

That is a reasonable policy for arbitrary files. It is not a good policy for a
knowledge base, where an important line should not vanish because another
machine later uploaded a competing copy of the same file.

[Git](https://git-scm.com/) takes the conservative approach: when it cannot
determine the intended merge, it asks a human to resolve the conflict block by
block. That is exactly what a version-control system should do, but it is too
much ceremony for a document library that is expected to synchronize in the
background. This tool needs deterministic conflict rules, automatic
convergence, and preservation of concurrent text edits at a finer level than
whole files.

### Why a CRDT?

Two major families of algorithms are commonly used for collaborative editing:
[Operational Transformation](https://en.wikipedia.org/wiki/Operational_transformation)
(OT) and [Conflict-free Replicated Data Types](https://crdt.tech/) (CRDTs). OT
came first and became widely known through real-time editors such as
Google Docs.

OT is usually deployed as part of a coordinated editing session: known clients
produce operations against a shared history, and a service continually orders
or transforms those operations. That model works extremely well for a
purpose-built online editor. It is a poor fit for onelastleaf, where a document
may be edited offline for a long time, where synchronization may happen only
after an explicit request, and where the editor might be
[Neovim](https://neovim.io/), a mobile app, or any other program capable of
writing a normal text file. onelastleaf cannot require every editor to
understand its collaboration protocol.

A CRDT fits those constraints. Replicas can accept edits independently, carry
their own history, and converge when they eventually exchange updates. That is
why onelastleaf uses Loro: it provides the CRDT foundation while allowing the
user-facing document library to remain a tree of ordinary, directly editable
files.

Synchronization moves the documents around. Plugins are for doing something
with their contents.

### Why plugins?

Storing notes is only part of the job. A useful document library also needs ways
to ingest, reshape, and export them.

A document might need to be split into knowledge cards and sent to
[Anki](https://apps.ankiweb.net/) for spaced-repetition training. Notes taken
while reading a book might need to be turned into a coherent, well-structured
document. On a phone, where writing long passages is inconvenient, fragments
might be captured through speech and processed later.

Those jobs should not all live in the core. They interpret and transform
documents in different ways. The plugin system allows each workflow to be
implemented independently, in any suitable language, without turning the
document library itself into a collection of hard-coded integrations.

**In one sentence:** onelastleaf is a multi-device document library with
CRDT-based conflict resolution and plugins for document workflows.

## Quick start

Download `oll` from the [GitHub Releases](https://github.com/onelastleaf/onelastleaf/releases)
page and place it somewhere on `PATH`. Then initialize a node:

```sh
oll init laptop
```

This creates the configuration, node identity, working tree, and storage
directories using the platform defaults. Before starting the daemon, generate a
network key:

```sh
oll psk
```

Copy the complete value printed by the command. On Linux, enter the default
config root and open the generated `config.lua` with an editor:

```sh
cd ~/.config/oll/
nano config.lua # or: vim config.lua
```

`config.lua` returns one top-level table. Find its `node` table, replace the
generated `network_key = nil`, and paste the copied value as a quoted Lua
string:

```lua
return {
    format_version = 1,

    node = {
        -- Keep the other generated fields unchanged.
        -- ...

        network_key = "<network_key>",
    },
}
```

Replace `<network_key>` itself rather than keeping the angle brackets. Every
node that should synchronize with this one must receive the exact same value.

Storing the key in a separate file is the recommended approach:

```sh
cd /absolute/path/to/config-root
oll psk > network.key
$EDITOR config.lua
# Set: network_key = oll.read_network_key("/absolute/path/to/config-root/network.key")
```

Start the daemon in the foreground:

```sh
oll run
```

The default working tree is the platform Documents directory under `oll`—on a
typical Linux system, `~/Documents/oll`. Files placed there remain ordinary
files and can be edited with any editor. oll watches the tree and imports
changes into its authoritative replica store.

To run the daemon in the background instead:

```sh
oll start
oll status
oll stop
```

Synchronization between nodes requires a shared network key and at least one
configured `listen` or `connect` endpoint. The complete setup guide and command
reference are available at [onelastleaf.org](https://onelastleaf.org).

## Configuration

An oll deployment keeps three user-owned files in its config root. The examples
below show their complete shapes with representative Linux paths and generated
UUIDs; `oll init` writes the actual platform paths and identities.

### `config.lua`

```lua
return {
    format_version = 1, -- 1; configuration schema version.

    node = {
        -- Ordinary files edited by the user and watched by oll.
        replica_root = "/home/alice/Documents/oll", -- OS path; absolute or config-root-relative.

        -- Authoritative replica data. Use "sqlite" with path, or
        -- "postgres" with url = oll.getenv("OLL_POSTGRES_URL").
        replica_store = {
            driver = "sqlite", -- "sqlite" | "postgres"; selects the SQL backend.
            path = "/home/alice/.local/share/oll/stores/<node-id>/replica.sqlite3", -- SQLite only; required for "sqlite" and forbidden for "postgres".
            -- url = oll.getenv("OLL_POSTGRES_URL"), -- PostgreSQL only; required for "postgres" and forbidden for "sqlite".
        },

        -- Structured daemon logs and verified plugin output files.
        log_dir = "/home/alice/.local/state/oll", -- OS path; absolute or config-root-relative.
        artifact_download_dir = "/home/alice/Downloads/oll", -- OS path; loaded at startup.

        -- Local bind address, or nil when this node does not accept connections.
        listen = nil, -- nil | "IP:port"; exactly one local bind endpoint.

        -- Remote peers. Every entry is an explicit oll://host:port URL.
        connect = {}, -- { "oll://host:port", ... }; an ordered list of peers.

        -- Raw Lua bytes shared by peers; required when listen/connect is used.
        network_key = nil, -- nil | byte string; no trimming or text normalization.
    },
}
```

`replica_root` is the editable working tree; `replica_store` is the authoritative
SQLite or PostgreSQL state and must be kept separate from it. `log_dir` and
`artifact_download_dir` may be moved independently, but none of the oll-managed
locations may overlap the watched tree. `listen` accepts one local socket
address such as `"0.0.0.0:17384"`; `connect` accepts any number of explicit
`oll://host:port` targets. Relative filesystem paths are resolved from the
config root. The file is trusted executable Lua, but its returned table is
strict: missing, unknown, or wrongly typed fields are rejected.

### `node.json`

```json
{
  "format_version": 1,
  "node_id": "9ba4a1aa-4c7d-4b11-b902-3155cf8ca5f3",
  "node_name": "laptop"
}
```

`node_id` is the canonical UUID-v4 identity generated by `oll init`.
`node_name` is its human-readable, lower-case DNS-label name and is the value
shown by peer status and CLI selectors. Both fields form one identity pair.
Editing either one deliberately changes this node's network identity; peers
that already know the old pairing may reject it as an identity collision. The
daemon hot-loads a valid replacement, but malformed JSON, unknown fields,
noncanonical UUIDs, and unsupported `format_version` values are rejected.

### `replica.json`

```json
{
  "format_version": 1,
  "replica_id": "44d62c47-0d82-42f0-a767-e3d6d5e75858"
}
```

This file is absent immediately after `oll init` and appears only when the node
creates, imports, or bootstraps its first replica. `replica_id` is the canonical
UUID-v4 identity of the logical document library, independent of `node_id`.
Changing it deliberately reidentifies that library; it does not create a copy,
move the store, or rewrite document history, and existing peers will normally
report a replica mismatch. oll coordinates valid runtime replacements with the
SQL cache and rejects malformed JSON, unknown fields, noncanonical UUIDs, and
unsupported versions.

See [onelastleaf.org](https://onelastleaf.org) for platform paths, storage
layout rules, PostgreSQL configuration, identity recovery, and the complete Lua
contract.

## Installation

Prebuilt releases for supported platforms are published on the
[GitHub Releases](https://github.com/onelastleaf/onelastleaf/releases) page.
Download the appropriate archive, extract the `oll` executable, and place it on
`PATH`. The current node runtime supports Linux and macOS; Windows support has
not yet been implemented.

## Project layout

```text
onelastleaf/
├── src/
│   ├── cli/             command parsing and intent validation
│   ├── configuration/   trusted Lua configuration runtime
│   ├── node/            daemon lifecycle, Admin API, and logging
│   ├── replica/         working tree, CRDT state, store, and snapshots
│   ├── sync/            Noise transport and replica synchronization
│   └── plugin/          packages, processes, jobs, and artifacts
├── proto/oll/           protobuf and gRPC contracts
├── docs/                architecture and behavioral specifications
├── tests/               executable and CLI integration tests
├── build.rs             protobuf build integration
└── Cargo.toml           Rust package and dependencies
```

The documents under [`docs/`](docs/README.md) are the source of truth for
architecture and behavior. Wire contracts live under [`proto/oll/`](proto/oll/).

## License

onelastleaf is distributed under the
[GNU General Public License v3.0](LICENSE).
