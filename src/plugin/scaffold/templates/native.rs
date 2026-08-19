use super::{GeneratedFile, Recipe, file};

pub(super) fn rust() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Rust",
            checkout: "source",
            dependencies: &[(
                "cargo",
                "Install the Rust toolchain and ensure cargo is in PATH.",
            )],
            steps: &[&[
                "cargo",
                "install",
                "--locked",
                "--path",
                "{source}",
                "--root",
                "{install}",
            ]],
            runtime: &["{install}/bin/__OLL_PLUGIN_NAME__"],
            gitignore: "/target\n",
        },
        vec![
            file(
                "Cargo.toml",
                r#"[package]
name = "__OLL_PLUGIN_NAME__"
version = "0.1.0"
edition = "2024"

[dependencies]
onelastleaf-plugin-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
"#,
            ),
            file(
                "src/lib.rs",
                r#"use onelastleaf_plugin_sdk::protocol::{ConfigValue, config_value};

pub fn echo(arguments: Vec<String>) -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::StringValue(arguments.join(" "))),
    }
}
"#,
            ),
            file(
                "src/main.rs",
                r#"use onelastleaf_plugin_sdk::{ActionResult, Plugin, SdkError};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    Plugin::builder("__OLL_PLUGIN_ID__", "0.1.0")
        .action("echo", "Return the supplied arguments", |_, arguments| async move {
            Ok(ActionResult {
                result: Some(__OLL_PLUGIN_MODULE__::echo(arguments)),
                artifacts: Vec::new(),
            })
        })?
        .build()?
        .run()
        .await
}
"#,
            ),
            file(
                "tests/echo.rs",
                r#"use onelastleaf_plugin_sdk::protocol::config_value;

#[test]
fn echo_preserves_argument_order() {
    let value = __OLL_PLUGIN_MODULE__::echo(vec!["one".into(), "two".into()]);
    assert!(matches!(
        value.kind,
        Some(config_value::Kind::StringValue(value)) if value == "one two"
    ));
}
"#,
            ),
        ],
    )
}

pub(super) fn go() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Go",
            checkout: "source",
            dependencies: &[("go", "Install Go and ensure go is in PATH.")],
            steps: &[&[
                "go",
                "build",
                "-o",
                "{install}/__OLL_PLUGIN_NAME__",
                "./cmd/plugin",
            ]],
            runtime: &["{install}/__OLL_PLUGIN_NAME__"],
            gitignore: "/__OLL_PLUGIN_NAME__\n/coverage.out\n",
        },
        vec![
            file(
                "go.mod",
                r#"module example.com/__OLL_PLUGIN_NAME__

go 1.24

require github.com/onelastleaf/go-plugin-sdk v0.1.0
"#,
            ),
            file(
                "echo/echo.go",
                r#"package echo

import (
	"strings"

	plugin "github.com/onelastleaf/go-plugin-sdk"
)

func Run(_ plugin.ActionContext, arguments []string) (plugin.ActionResult, error) {
	return plugin.StringResult(strings.Join(arguments, " ")), nil
}
"#,
            ),
            file(
                "echo/echo_test.go",
                r#"package echo

import "testing"

func TestRun(t *testing.T) {
	result, err := Run(nil, []string{"one", "two"})
	if err != nil || result.String() != "one two" {
		t.Fatalf("unexpected result: %#v, %v", result, err)
	}
}
"#,
            ),
            file(
                "cmd/plugin/main.go",
                r#"package main

import (
	"context"
	"log"

	"example.com/__OLL_PLUGIN_NAME__/echo"
	plugin "github.com/onelastleaf/go-plugin-sdk"
)

func main() {
	runtime, err := plugin.New("__OLL_PLUGIN_ID__", "0.1.0").
		Action("echo", "Return the supplied arguments", echo.Run).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
"#,
            ),
        ],
    )
}

pub(super) fn cpp() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "C++",
            checkout: "source",
            dependencies: &[(
                "cmake",
                "Install CMake, a C++ compiler, protobuf, and gRPC.",
            )],
            steps: &[
                &[
                    "cmake",
                    "-S",
                    "{source}",
                    "-B",
                    "{source}/build",
                    "-DCMAKE_BUILD_TYPE=Release",
                    "-DCMAKE_INSTALL_PREFIX={install}",
                ],
                &[
                    "cmake",
                    "--build",
                    "{source}/build",
                    "--target",
                    "install",
                    "--parallel",
                ],
            ],
            runtime: &["{install}/bin/__OLL_PLUGIN_NAME__"],
            gitignore: "/build\n",
        },
        vec![
            file(
                "CMakeLists.txt",
                r#"cmake_minimum_required(VERSION 3.24)
project(__OLL_PLUGIN_NAME__ VERSION 0.1.0 LANGUAGES CXX)

include(FetchContent)
FetchContent_Declare(
  onelastleaf_plugin_sdk
  GIT_REPOSITORY https://github.com/onelastleaf/cpp-plugin-sdk.git
  GIT_TAG v0.1.0
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(onelastleaf_plugin_sdk)

add_executable(__OLL_PLUGIN_NAME__ src/main.cpp src/echo.cpp)
target_compile_features(__OLL_PLUGIN_NAME__ PRIVATE cxx_std_20)
target_link_libraries(__OLL_PLUGIN_NAME__ PRIVATE onelastleaf::plugin_sdk)
install(TARGETS __OLL_PLUGIN_NAME__ RUNTIME DESTINATION bin)

include(CTest)
if(BUILD_TESTING)
  add_executable(echo_test tests/echo_test.cpp src/echo.cpp)
  target_include_directories(echo_test PRIVATE src)
  add_test(NAME echo_test COMMAND echo_test)
endif()
"#,
            ),
            file(
                "src/echo.hpp",
                "#pragma once\n\n#include <string>\n#include <vector>\n\nstd::string echo(const std::vector<std::string>& arguments);\n",
            ),
            file(
                "src/echo.cpp",
                r#"#include "echo.hpp"

std::string echo(const std::vector<std::string>& arguments) {
  std::string output;
  for (const auto& value : arguments) {
    if (!output.empty()) output += ' ';
    output += value;
  }
  return output;
}
"#,
            ),
            file(
                "src/main.cpp",
                r#"#include "echo.hpp"

#include <onelastleaf/plugin_sdk.hpp>

int main() {
  onelastleaf::Plugin plugin{"__OLL_PLUGIN_ID__", "0.1.0"};
  plugin.action("echo", "Return the supplied arguments", [](auto&, const auto& arguments) {
    return onelastleaf::ActionResult::string(echo(arguments));
  });
  return plugin.run();
}
"#,
            ),
            file(
                "tests/echo_test.cpp",
                r#"#include "echo.hpp"
#include <cassert>

int main() {
  assert(echo({"one", "two"}) == "one two");
}
"#,
            ),
        ],
    )
}

pub(super) fn dotnet() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "C#/.NET",
            checkout: "source",
            dependencies: &[(
                "dotnet",
                "Install the .NET SDK and ensure dotnet is in PATH.",
            )],
            steps: &[&[
                "dotnet",
                "publish",
                "{source}/src/Plugin.csproj",
                "--configuration",
                "Release",
                "--output",
                "{install}",
            ]],
            runtime: &["dotnet", "{install}/Plugin.dll"],
            gitignore: "/**/bin\n/**/obj\n",
        },
        vec![
            file(
                "src/Plugin.csproj",
                r#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Onelastleaf.PluginSdk" Version="0.1.0" />
  </ItemGroup>
</Project>
"#,
            ),
            file(
                "src/Echo.cs",
                "namespace ExamplePlugin;\n\npublic static class Echo\n{\n    public static string Run(IEnumerable<string> arguments) => string.Join(\" \", arguments);\n}\n",
            ),
            file(
                "src/Program.cs",
                r#"using ExamplePlugin;
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("__OLL_PLUGIN_ID__", "0.1.0")
    .Action("echo", "Return the supplied arguments",
        (_, arguments) => Task.FromResult(ActionResult.String(Echo.Run(arguments))));
await plugin.RunAsync();
"#,
            ),
            file(
                "tests/Plugin.Tests.csproj",
                r#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><TargetFramework>net10.0</TargetFramework></PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.14.1" />
    <PackageReference Include="xunit" Version="2.9.3" />
    <PackageReference Include="xunit.runner.visualstudio" Version="3.1.4" />
    <ProjectReference Include="../src/Plugin.csproj" />
  </ItemGroup>
</Project>
"#,
            ),
            file(
                "tests/EchoTests.cs",
                "using ExamplePlugin;\nusing Xunit;\n\npublic sealed class EchoTests\n{\n    [Fact]\n    public void PreservesOrder() => Assert.Equal(\"one two\", Echo.Run([\"one\", \"two\"]));\n}\n",
            ),
        ],
    )
}

pub(super) fn swift() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Swift",
            checkout: "source",
            dependencies: &[
                ("swift", "Install Swift and ensure swift is in PATH."),
                (
                    "install",
                    "Install a POSIX install utility and ensure it is in PATH.",
                ),
            ],
            steps: &[
                &[
                    "swift",
                    "build",
                    "--package-path",
                    "{source}",
                    "--configuration",
                    "release",
                    "--scratch-path",
                    "{source}/build",
                ],
                &[
                    "install",
                    "-m",
                    "755",
                    "{source}/build/release/__OLL_PLUGIN_NAME__",
                    "{install}/__OLL_PLUGIN_NAME__",
                ],
            ],
            runtime: &["{install}/__OLL_PLUGIN_NAME__"],
            gitignore: "/.build\n/.swiftpm\n/build\n",
        },
        vec![
            file(
                "Package.swift",
                r#"// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "__OLL_PLUGIN_NAME__",
    platforms: [.macOS(.v15)],
    products: [
        .executable(name: "__OLL_PLUGIN_NAME__", targets: ["PluginMain"])
    ],
    dependencies: [
        .package(url: "https://github.com/onelastleaf/swift-plugin-sdk.git", exact: "0.1.0")
    ],
    targets: [
        .executableTarget(name: "PluginMain", dependencies: [
            .product(name: "OnelastleafPluginSDK", package: "swift-plugin-sdk")
        ]),
        .testTarget(name: "PluginTests", dependencies: ["PluginMain"])
    ]
)
"#,
            ),
            file(
                "Sources/PluginMain/Echo.swift",
                "public func echo(_ arguments: [String]) -> String { arguments.joined(separator: \" \") }\n",
            ),
            file(
                "Sources/PluginMain/main.swift",
                r#"import Foundation
import OnelastleafPluginSDK

do {
  let plugin = try Plugin(id: "__OLL_PLUGIN_ID__", version: "0.1.0")
    .action(name: "echo", description: "Return the supplied arguments") { _, arguments in
      .string(echo(arguments))
    }
  try await plugin.run()
} catch {
  FileHandle.standardError.write(Data("plugin failed: \(error)\n".utf8))
  exit(EXIT_FAILURE)
}
"#,
            ),
            file(
                "Tests/PluginTests/EchoTests.swift",
                "import Testing\n@testable import PluginMain\n\n@Test func preservesOrder() { #expect(echo([\"one\", \"two\"]) == \"one two\") }\n",
            ),
        ],
    )
}

pub(super) fn haskell() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Haskell",
            checkout: "install",
            dependencies: &[(
                "cabal",
                "Install GHC and cabal-install and ensure cabal is in PATH.",
            )],
            steps: &[&[
                "cabal",
                "install",
                "exe:__OLL_PLUGIN_NAME__",
                "--project-dir={install}",
                "--install-method=copy",
                "--installdir={install}",
                "--overwrite-policy=always",
            ]],
            runtime: &["{install}/__OLL_PLUGIN_NAME__"],
            gitignore: "/dist-newstyle\n",
        },
        vec![
            file("cabal.project", "packages: .\n"),
            file(
                "__OLL_PLUGIN_NAME__.cabal",
                r#"cabal-version: 3.0
name: __OLL_PLUGIN_NAME__
version: 0.1.0.0
build-type: Simple

executable __OLL_PLUGIN_NAME__
  main-is: Main.hs
  hs-source-dirs: app, src
  other-modules: Echo
  build-depends: base >=4.18 && <5, text, onelastleaf-plugin-sdk ==0.1.0
  default-language: GHC2021
  default-extensions: OverloadedStrings

test-suite echo-test
  type: exitcode-stdio-1.0
  main-is: EchoTest.hs
  hs-source-dirs: test, src
  other-modules: Echo
  build-depends: base >=4.18 && <5, text
  default-language: GHC2021
  default-extensions: OverloadedStrings
"#,
            ),
            file(
                "src/Echo.hs",
                "module Echo (echo) where\n\nimport Data.Text (Text)\nimport qualified Data.Text as Text\n\necho :: [Text] -> Text\necho = Text.unwords\n",
            ),
            file(
                "app/Main.hs",
                r#"module Main (main) where

import Echo (echo)
import qualified Data.Text as Text
import Onelastleaf.PluginSDK

main :: IO ()
main = either (fail . Text.unpack) runPlugin $ do
  plugin <- newPlugin "__OLL_PLUGIN_ID__" "0.1.0"
  addAction
    (Action "echo" "Return the supplied arguments" $ \_ arguments ->
      pure (stringResult (echo arguments)))
    plugin
"#,
            ),
            file(
                "test/EchoTest.hs",
                "module Main (main) where\n\nimport Echo (echo)\n\nmain :: IO ()\nmain = if echo [\"one\", \"two\"] == \"one two\" then pure () else fail \"echo mismatch\"\n",
            ),
        ],
    )
}
