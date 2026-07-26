# Plugin packaging and installation

## Scope

Plugins may be implemented in any language that can host the protobuf gRPC
service. oll does not infer a language or classify a plugin as compiled or
interpreted. A source package declares commands that prepare and launch it; a
release package declares downloadable, ready-to-launch artifacts.

The package contract has three files with separate owners:

| File | Owner | Purpose |
| --- | --- | --- |
| repository-root `oll.toml` | plugin publisher | identity, source recipe, dependencies, and entrypoint |
| repository-root `oll.json` | plugin publisher | release artifact URLs, targets, sizes, hashes, and entrypoints |
| `<config-root>/plugins.lua` | oll user and oll CLI | desired plugin installation declarations |

There are no semantic-version ranges. A publisher exposes distinct maintained
versions through branches such as `release/v0.3.1`. Users select a branch or an
exact Git revision. Git tags are not part of oll's installation model.

## Git remotes and selection

`GitRemote` accepts Git URL forms and SCP-style SSH remotes, including:

```text
https://github.com/example/oll-anki.git
ssh://git@github.com/example/oll-anki.git
git@github.com:example/oll-anki.git
```

The value is parsed as a Git remote, not a generic web URL, and its original
spelling is retained for Git. `--branch` and `--rev` are mutually exclusive.
Without either option, Git's remote default branch is selected.

Source and release installation use the same selected branch or revision:

- source mode reads `oll.toml` and executes its source recipe;
- release mode reads `oll.toml` for identity and `oll.json` for direct artifact
  URLs.

oll does not query or normalize GitHub, GitLab, Codeberg, Gitea, or Forgejo
Release APIs. A publisher may host artifacts on those services, but `oll.json`
contains the complete download URL, so the hosting platform is not part of the
installer's domain model.

## Publisher source manifest

`oll.toml` is UTF-8 TOML with an explicit format version. The initial logical
shape is:

```toml
format_version = 1

[plugin]
id = "oll.anki"
protocol_fingerprint = "hex-encoded-schema-fingerprint"

[[source.dependencies]]
executable = "cargo"
hint = "Install the Rust toolchain and ensure cargo is in PATH."

[[source.steps]]
argv = ["cargo", "install", "--locked", "--path", "{source}", "--root", "{install}"]

[source.entrypoint]
argv = ["{install}/bin/oll-anki"]
```

A recipe for an interpreted plugin follows the same schema; only its dependency,
steps, and entrypoint argv differ. The manifest has no compiled/interpreted or
language enum.

Commands are argv arrays, never implicit shell programs. Placeholders are an
explicit allowlist resolved by oll; unknown placeholders are errors. The first
implementation provides `{source}`, `{staging}`, and `{install}` and does not
perform shell expansion, command substitution, or environment interpolation.

Dependencies declare only executable capabilities:

```toml
[[source.dependencies]]
executable = "python3"
hint = "Install Python 3 and ensure python3 is in PATH."
```

oll checks whether each executable can be resolved using the current platform's
PATH rules, including `PATHEXT` on Windows. It does not invoke a package manager,
install dependencies, execute `--version`, parse version output, or enforce
dependency versions. A missing executable aborts installation and displays the
publisher's required non-empty `hint`. Headers, libraries, and distribution
package names are not separately modeled; failures involving them are reported
from the recipe step and its retained build log.

Source recipes execute trusted third-party code. They run in a private staging
directory and MUST NOT receive root privileges from oll. The completed install
is published by atomic rename only after every step and the entrypoint check
succeeds. Failure leaves no partially active installation.

## Publisher release index

`oll.json` is UTF-8 JSON read from the selected branch or revision. Its initial
logical shape is:

```json
{
  "format_version": 1,
  "plugin_id": "oll.anki",
  "protocol_fingerprint": "hex-encoded-schema-fingerprint",
  "artifacts": [
    {
      "target": "x86_64-unknown-linux-gnu",
      "url": "https://downloads.example/oll-anki-x86_64-unknown-linux-gnu.tar.gz",
      "archive": "tar.gz",
      "size_bytes": 123456,
      "sha256": "hex-encoded-sha256",
      "entrypoint": "bin/oll-anki"
    }
  ]
}
```

The `plugin_id` and protocol fingerprint MUST match `oll.toml`. Exactly one
artifact must match the local target; zero and duplicate matches are errors.
Publishers use canonical target names such as:

```text
x86_64-unknown-linux-gnu
x86_64-unknown-linux-musl
aarch64-unknown-linux-gnu
x86_64-apple-darwin
aarch64-apple-darwin
x86_64-pc-windows-msvc
aarch64-pc-windows-msvc
```

Publisher metadata MUST use canonical architecture names. Input aliases such as
`amd64` and `x64` may be recognized only to produce a targeted validation error;
they are not silently accepted as canonical publication metadata.

Linux artifacts use `.tar.gz`; Windows and Darwin artifacts use `.zip`. The
declared archive type and filename suffix must agree. Extraction rejects absolute
paths, `..` traversal, duplicate entries, links that escape staging, unsupported
entry types, and configured compressed or expanded size limits. The declared
size and SHA-256 are checked before extraction. SHA-256 detects corruption or a
mismatched download; it is not a publisher signature.

A release archive includes its `oll.toml`. The embedded identity, protocol
fingerprint, and entrypoint must match the index before atomic publication.

## User installation declarations

`<config-root>/plugins.lua` is the durable source of desired plugin
installations. It is a data-only Lua module keyed by `PluginId`:

```lua
return {
    ["oll.anki"] = {
        remote = "git@github.com:example/oll-anki.git",
        mode = "release",
        branch = "release/v0.3.1",
    },
    ["oll.pdf"] = {
        remote = "https://codeberg.org/example/oll-pdf.git",
        mode = "source",
        branch = "main",
    },
}
```

`mode` is `source` or `release` and defaults to `source`. `branch` and `rev` are
optional and mutually exclusive. There is no `version`, version range, tag, or
hosting-platform field.

The configuration runtime is LuaJIT only. The Rust implementation uses the
`mlua` crate with its LuaJIT backend for Lua/Rust value conversion. Other Lua
implementations are not supported by the first version.

Because oll rewrites this file, it accepts only one return statement containing
a recursively literal table of strings, booleans, integers, lists, and maps.
Calls, operators, local bindings, loops, conditions, functions, `require`,
metatables, userdata, threads, bytecode, cyclic tables, duplicate plugin IDs,
unknown fields, and values outside the schema are errors. Syntax restriction is
validated before the chunk is evaluated by LuaJIT; recursive value validation
is then performed after conversion. oll never treats arbitrary executable Lua
as editable installation data.

Writes take a deployment-local lock, serialize keys in stable order, write a
temporary sibling file, fsync it, and atomically rename it over `plugins.lua`.
A parse or validation failure never rewrites the original file.

## Equivalent installation entrypoint

Both public installation forms converge on the same reconciliation entrypoint:

```text
oll plugin install
oll plugin install <git-remote> [--branch <branch> | --rev <revision>]
  [--source | --release]
```

The no-argument form validates and installs the declarations in `plugins.lua`.
The one-remote form first resolves `oll.toml` to obtain the `PluginId`, then:

1. adds the declaration when the ID is absent;
2. leaves an identical declaration unchanged;
3. when the ID exists with another declaration, asks whether to overwrite it;
4. on confirmation, atomically writes the replacement;
5. invokes the same no-argument reconciliation from the newly read file.

A negative answer, EOF, or inability to read an interactive answer leaves the
file and installation unchanged. A successful declaration write is not rolled
back when installation later fails: the file records desired state, and a later
`oll plugin install` retries it. This is identical to writing the declaration by
hand before running the no-argument command.

The daemon owns mutation and reconciliation so concurrent Admin clients cannot
race file updates. Resolution work performed before mutation may be reused after
the file is reread, but the persisted declaration, not client argv, is the input
to installation.

`oll plugin validate` is a bounded local configuration command. It locates
`<config-root>/plugins.lua`, validates its literal-only syntax and complete
schema, reports every safely recoverable diagnostic in source order, and exits
without opening the Admin API, accessing a remote, executing a recipe, or
changing a file.

## Installation diagnostics

Installation and validation expose stable error codes with structured context
and an optional actionable hint. Initial codes include:

| Code | Meaning |
| --- | --- |
| `git_remote_invalid` | Remote syntax is not accepted by `GitRemote`. |
| `git_fetch_failed` | The selected branch or revision could not be fetched. |
| `git_selection_not_found` | The requested branch or revision does not exist. |
| `plugin_config_missing` | `plugins.lua` does not exist where required. |
| `plugin_config_syntax` | `plugins.lua` is not a literal-only data module. |
| `plugin_config_schema` | A returned field or value violates the schema. |
| `plugin_config_duplicate` | A plugin ID is declared more than once. |
| `manifest_missing` | `oll.toml` is absent from the selected repository state. |
| `manifest_invalid` | `oll.toml` is malformed or internally inconsistent. |
| `manifest_version_unsupported` | Its format version is unsupported. |
| `dependency_missing` | A declared executable is not resolvable in PATH. |
| `recipe_step_failed` | A source recipe argv exited unsuccessfully. |
| `recipe_output_missing` | The declared source entrypoint was not produced. |
| `release_index_missing` | Release mode cannot find `oll.json`. |
| `release_index_invalid` | Its JSON or internal contract is invalid. |
| `artifact_unavailable` | No artifact matches the canonical local target. |
| `artifact_ambiguous` | Multiple artifacts match the local target. |
| `artifact_download_failed` | The selected direct URL could not be downloaded. |
| `artifact_checksum_mismatch` | Size or SHA-256 verification failed. |
| `archive_unsafe` | Archive structure violates extraction constraints. |
| `entrypoint_invalid` | The installed entrypoint is missing or unusable. |
| `protocol_incompatible` | The package targets another protocol fingerprint. |
| `install_publish_failed` | Atomic publication of the staged install failed. |

Diagnostics identify the phase, plugin ID when known, sanitized remote,
branch/revision, target, and retained build-log path. Secrets and URL credentials
are redacted. A missing dependency displays the exact publisher hint. Errors do
not invent package-manager commands or version advice.
