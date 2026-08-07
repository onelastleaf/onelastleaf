<div align="center">

  ![logo](https://preview.github.sov710.org/onelastleaf/onelastleaf-logo.svg)

  # onelastleaf

  [![License](https://img.shields.io/github/license/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=bb9af7)](LICENSE)
  [![Last Commit](https://img.shields.io/github/last-commit/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=7aa2f7)](https://github.com/SOV710/nvim-config/commits/main)
  [![Stars](https://img.shields.io/github/stars/onelastleaf/onelastleaf?style=flat-square&labelColor=1a1b26&color=7aa2f7&logo=github&logoColor=white)](https://github.com/SOV710/nvim-config/stargazers)
</div>

onelastleaf is a self-hosted, CRDT-powered document library for multi-device
synchronization, with plugins for extending what the library can do.

## Why the name *onelastleaf*?

The first inspiration was *One Last Kiss*, the theme song from the final
*Evangelion* film.

The most recognizable organization in *Evangelion* is NERV, whose emblem
features a fig leaf. The interpretation behind the name is summarized well
here:

> I always assumed it was a fig leaf. When Adam & Eve ate of the fruit of
> knowledge and realised they were naked, they made clothing out of fig leaves.
> Biblically it was the first thing human beings made for themselves. So I think
> the logo represents Nerv holding forbidden knowledge and creating something
> out of it, namely the Evas. --- Hideous-Kojima, Reddit

Calling the project *One Last Kiss* would have made the reference a little too
obvious, so *kiss* became *leaf*. A leaf is part of a book, a fig leaf is part of
the NERV imagery, and both point back to the same idea: knowledge turned into
something that can be preserved and used to create.

## What is onelastleaf?

onelastleaf is being built around three ideas:

- **Multi-device, peer-to-peer synchronization.** Keep the same document
  library on a desktop, laptop, phone, or another machine without making one
  device the permanent authority.
- **CRDT-based conflict resolution.** Concurrent edits converge without
  reducing every text conflict to “choose this entire file or that one.” Text
  documents are merged through [Loro](https://loro.dev/); binary files are
  synchronized but do not receive text-level CRDT merging. The working tree
  remains a directory of ordinary files that can be edited with ordinary tools.
- **A plugin system.** Turn documents into inputs and outputs for other
  workflows: generate study material, transform notes, call external tools, or
  build entirely new ways to work with a library.

The project is self-hosted. There is no onelastleaf SaaS service at present;
there may be one in the future, but a hosted service would first require a
proper end-to-end encryption design. The current protocol encrypts and
authenticates peer connections with a shared network key. It is designed for
trusted members of a sync network, not to hide data from an untrusted hosted
service or a malicious member that possesses that key.

## Why build another document library?

Memory is not a database. An idea should not be trusted to remain available just
because it once felt unforgettable. Knowledge becomes durable when it is
written down and revisited repeatedly. The simplest way to do that is to write
notes—or, in more programmer-friendly language, to write documentation.

At first, that sounds like a solved problem: open any editor and keep a folder
of Markdown files. That works until the folder needs to follow its user. A
document library should remain readable and editable from a phone, a lightweight
laptop, or whichever machine happens to be nearby—not only from one desktop.

That creates the first requirement: the document library must synchronize
across devices.

### Why not just use Syncthing?

File synchronization and conflict resolution are different problems.
Syncthing and similar tools are excellent at moving files between machines, but
their unit of synchronization is fundamentally the file. When two devices edit
the same document independently, a file-level synchronizer cannot generally
combine both sets of textual changes into one result without producing conflict
copies or choosing one version.

That is a reasonable policy for arbitrary files. It is not a good policy for a
knowledge base, where an important line should not vanish because another
machine later uploaded a competing copy of the same file.

Git takes the conservative approach: when it cannot determine the intended
merge, it asks a human to resolve the conflict block by block. That is exactly
what a version-control system should do, but it is too much ceremony for a
document library that is expected to synchronize in the background. This tool
needs deterministic conflict rules, automatic convergence, and preservation of
concurrent text edits at a finer level than whole files.

### Why a CRDT?

Two major families of algorithms are commonly used for collaborative editing:
Operational Transformation (OT) and Conflict-free Replicated Data Types
(CRDTs). OT came first and became widely known through real-time editors such as
Google Docs.

OT is usually deployed as part of a coordinated editing session: known clients
produce operations against a shared history, and a service continually orders
or transforms those operations. That model works extremely well for a
purpose-built online editor. It is a poor fit for onelastleaf, where a document
may be edited offline for a long time, where synchronization may happen only
after an explicit request, and where the editor might be Neovim, a mobile app,
or any other program capable of writing a normal text file. onelastleaf cannot
require every editor to understand its collaboration protocol.

A CRDT fits those constraints. Replicas can accept edits independently, carry
their own history, and converge when they eventually exchange updates. That is
why onelastleaf uses Loro: it provides the CRDT foundation while allowing the
user-facing document library to remain a tree of ordinary, directly editable
files.

Synchronization gets the documents where they need to be. It does not yet make
use of what is inside them.

### Why plugins?

Storing knowledge is only one half of working with it. A document library also
needs many ways to receive knowledge and produce something useful from it.

A document might need to be split into knowledge cards and sent to Anki for
spaced-repetition training. Notes taken while reading a book might need to be
turned into a coherent, well-structured document. On a phone, where writing long
passages is inconvenient, fragments might be captured through speech and
processed later.

Those capabilities do not all belong in the core. They are different ways of
interpreting, transforming, and producing documents. The plugin system exists
so that each workflow can be implemented independently, in any suitable
language, without turning the document library itself into a collection of
hard-coded integrations.

In one sentence: **onelastleaf is a multi-device document library with
CRDT-based conflict resolution and a plugin system for processing documents in
workflow-specific ways.**
