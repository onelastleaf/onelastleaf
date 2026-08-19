use super::{GeneratedFile, Recipe, file};

pub(super) fn javascript() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "JavaScript/Node.js",
            checkout: "install",
            dependencies: &[("npm", "Install Node.js with npm and ensure npm is in PATH.")],
            steps: &[&["npm", "install"]],
            runtime: &["node", "{install}/src/index.js"],
            gitignore: "/node_modules\n/coverage\n",
        },
        vec![
            file(
                "package.json",
                r#"{
  "name": "__OLL_PLUGIN_NAME__",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": { "test": "node --test" },
  "dependencies": { "@onelastleaf/plugin-sdk": "0.1.0" }
}
"#,
            ),
            file(
                "src/echo.js",
                "export function echo(arguments_) { return arguments_.join(' '); }\n",
            ),
            file(
                "src/index.js",
                r#"import { ActionResult, Plugin } from '@onelastleaf/plugin-sdk';
import { echo } from './echo.js';

await new Plugin('__OLL_PLUGIN_ID__', '0.1.0')
  .action('echo', 'Return the supplied arguments', async (_context, arguments_) =>
    ActionResult.string(echo(arguments_)))
  .run();
"#,
            ),
            file(
                "test/echo.test.js",
                "import assert from 'node:assert/strict';\nimport test from 'node:test';\nimport { echo } from '../src/echo.js';\n\ntest('echo preserves argument order', () => assert.equal(echo(['one', 'two']), 'one two'));\n",
            ),
        ],
    )
}

pub(super) fn typescript() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "TypeScript/Node.js",
            checkout: "install",
            dependencies: &[("npm", "Install Node.js with npm and ensure npm is in PATH.")],
            steps: &[&["npm", "install"]],
            runtime: &["node", "{install}/dist/index.js"],
            gitignore: "/node_modules\n/dist\n/coverage\n",
        },
        vec![
            file(
                "package.json",
                r#"{
  "name": "__OLL_PLUGIN_NAME__",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "prepare": "npm run build",
    "test": "node --import tsx --test test/*.test.ts"
  },
  "dependencies": { "@onelastleaf/plugin-sdk": "0.1.0" },
  "devDependencies": { "@types/node": "^24.0.0", "tsx": "^4.20.0", "typescript": "^5.9.0" }
}
"#,
            ),
            file(
                "tsconfig.json",
                r#"{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "outDir": "dist",
    "rootDir": "src",
    "strict": true
  },
  "include": ["src/**/*.ts"]
}
"#,
            ),
            file(
                "src/echo.ts",
                "export function echo(arguments_: string[]): string { return arguments_.join(' '); }\n",
            ),
            file(
                "src/index.ts",
                r#"import { ActionResult, Plugin } from '@onelastleaf/plugin-sdk';
import { echo } from './echo.js';

await new Plugin('__OLL_PLUGIN_ID__', '0.1.0')
  .action('echo', 'Return the supplied arguments', async (_context, arguments_) =>
    ActionResult.string(echo(arguments_)))
  .run();
"#,
            ),
            file(
                "test/echo.test.ts",
                "import assert from 'node:assert/strict';\nimport test from 'node:test';\nimport { echo } from '../src/echo.js';\n\ntest('echo preserves argument order', () => assert.equal(echo(['one', 'two']), 'one two'));\n",
            ),
        ],
    )
}

pub(super) fn python() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Python",
            checkout: "generation",
            dependencies: &[(
                "python3",
                "Install Python 3.11 or newer and ensure python3 is in PATH.",
            )],
            steps: &[
                &["python3", "-m", "venv", "{generation}/.venv"],
                &["{generation}/.venv/bin/pip", "install", "{generation}"],
            ],
            runtime: &["{generation}/.venv/bin/__OLL_PLUGIN_NAME__"],
            gitignore: "/.venv\n/__pycache__\n/.pytest_cache\n/*.egg-info\n",
        },
        vec![
            file(
                "pyproject.toml",
                r#"[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "__OLL_PLUGIN_NAME__"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["onelastleaf-plugin-sdk==0.1.0"]

[project.scripts]
__OLL_PLUGIN_NAME__ = "__OLL_PLUGIN_MODULE__.__main__:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
"#,
            ),
            file("src/__OLL_PLUGIN_MODULE__/__init__.py", ""),
            file(
                "src/__OLL_PLUGIN_MODULE__/echo.py",
                "def echo(arguments: list[str]) -> str:\n    return \" \".join(arguments)\n",
            ),
            file(
                "src/__OLL_PLUGIN_MODULE__/__main__.py",
                r#"import asyncio

from onelastleaf_plugin_sdk import ActionResult, Plugin
from .echo import echo

plugin = Plugin("__OLL_PLUGIN_ID__", "0.1.0")

@plugin.action("echo", "Return the supplied arguments")
async def echo_action(_context, arguments: list[str]) -> ActionResult:
    return ActionResult.string(echo(arguments))

def main() -> None:
    asyncio.run(plugin.run())

if __name__ == "__main__":
    main()
"#,
            ),
            file(
                "tests/test_echo.py",
                "from __OLL_PLUGIN_MODULE__.echo import echo\n\ndef test_echo_preserves_order() -> None:\n    assert echo([\"one\", \"two\"]) == \"one two\"\n",
            ),
        ],
    )
}

pub(super) fn elixir() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Elixir",
            checkout: "source",
            dependencies: &[
                ("mix", "Install Elixir with Mix and ensure mix is in PATH."),
                (
                    "install",
                    "Install a POSIX install utility and ensure it is in PATH.",
                ),
            ],
            steps: &[
                &["mix", "deps.get"],
                &["mix", "escript.build"],
                &[
                    "install",
                    "-m",
                    "755",
                    "{source}/__OLL_PLUGIN_NAME__",
                    "{install}/__OLL_PLUGIN_NAME__",
                ],
            ],
            runtime: &["{install}/__OLL_PLUGIN_NAME__"],
            gitignore: "/_build\n/deps\n/__OLL_PLUGIN_NAME__\n",
        },
        vec![
            file(
                "mix.exs",
                r#"defmodule ExamplePlugin.MixProject do
  use Mix.Project

  def project do
    [
      app: :__OLL_PLUGIN_MODULE__,
      version: "0.1.0",
      elixir: "~> 1.17",
      escript: [main_module: ExamplePlugin],
      deps: [{:onelastleaf_plugin_sdk, "== 0.1.0"}]
    ]
  end

  def application, do: [extra_applications: [:logger]]
end
"#,
            ),
            file(
                "lib/example_plugin.ex",
                r#"defmodule ExamplePlugin do
  alias Onelastleaf.PluginSDK.{ActionResult, Plugin}

  def main(_arguments) do
    Plugin.new!("__OLL_PLUGIN_ID__", "0.1.0")
    |> Plugin.action("echo", "Return the supplied arguments", fn _context, arguments ->
      ActionResult.string(Enum.join(arguments, " "))
    end)
    |> Plugin.run!()
  end

  def echo(arguments), do: Enum.join(arguments, " ")
end
"#,
            ),
            file("test/test_helper.exs", "ExUnit.start()\n"),
            file(
                "test/example_plugin_test.exs",
                "defmodule ExamplePluginTest do\n  use ExUnit.Case\n  test \"echo preserves argument order\" do\n    assert ExamplePlugin.echo([\"one\", \"two\"]) == \"one two\"\n  end\nend\n",
            ),
        ],
    )
}
