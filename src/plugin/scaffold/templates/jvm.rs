use super::{GeneratedFile, Recipe, file};

const RECIPE: Recipe = Recipe {
    language: "JVM",
    checkout: "source",
    dependencies: &[(
        "gradle",
        "Install Gradle and a Java 21 JDK and ensure gradle is in PATH.",
    )],
    steps: &[&[
        "gradle",
        "--project-dir",
        "{source}",
        "--no-daemon",
        "-PinstallDir={install}",
        "installDist",
    ]],
    runtime: &["{install}/bin/__OLL_PLUGIN_NAME__"],
    gitignore: "/.gradle\n/build\n",
};

fn settings() -> GeneratedFile {
    file(
        "settings.gradle.kts",
        "rootProject.name = \"__OLL_PLUGIN_NAME__\"\n",
    )
}

pub(super) fn java() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Java",
            ..RECIPE
        },
        vec![
            settings(),
            file(
                "build.gradle.kts",
                r#"plugins {
    application
    java
}

repositories { mavenCentral() }

dependencies {
    implementation("org.onelastleaf:onelastleaf-plugin-sdk-java:0.1.0")
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }
application { mainClass = "example.PluginMain" }
tasks.test { useJUnitPlatform() }
tasks.installDist {
    destinationDir = file(providers.gradleProperty("installDir").getOrElse("build/install"))
}
"#,
            ),
            file(
                "src/main/java/example/Echo.java",
                "package example;\n\nimport java.util.List;\n\npublic final class Echo {\n    private Echo() {}\n    public static String run(List<String> arguments) { return String.join(\" \", arguments); }\n}\n",
            ),
            file(
                "src/main/java/example/PluginMain.java",
                r#"package example;

import java.util.concurrent.CompletableFuture;
import org.onelastleaf.plugin.sdk.ActionResult;
import org.onelastleaf.plugin.sdk.Plugin;

public final class PluginMain {
    public static void main(String[] args) {
        Plugin.builder("__OLL_PLUGIN_ID__", "0.1.0")
            .action("echo", "Return the supplied arguments", (context, arguments) ->
                CompletableFuture.completedFuture(ActionResult.string(Echo.run(arguments))))
            .build()
            .run()
            .toCompletableFuture()
            .join();
    }
}
"#,
            ),
            file(
                "src/test/java/example/EchoTest.java",
                "package example;\n\nimport static org.junit.jupiter.api.Assertions.assertEquals;\nimport java.util.List;\nimport org.junit.jupiter.api.Test;\n\nfinal class EchoTest {\n    @Test void preservesOrder() { assertEquals(\"one two\", Echo.run(List.of(\"one\", \"two\"))); }\n}\n",
            ),
        ],
    )
}

pub(super) fn kotlin() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Kotlin",
            ..RECIPE
        },
        vec![
            settings(),
            file(
                "build.gradle.kts",
                r#"plugins {
    application
    kotlin("jvm") version "2.2.20"
}

repositories { mavenCentral() }
dependencies {
    implementation("org.onelastleaf:onelastleaf-plugin-sdk-kotlin:0.1.0")
    testImplementation(kotlin("test"))
}
kotlin { jvmToolchain(21) }
application { mainClass = "example.PluginMainKt" }
tasks.test { useJUnitPlatform() }
tasks.installDist {
    destinationDir = file(providers.gradleProperty("installDir").getOrElse("build/install"))
}
"#,
            ),
            file(
                "src/main/kotlin/example/Echo.kt",
                "package example\n\nfun echo(arguments: List<String>): String = arguments.joinToString(\" \")\n",
            ),
            file(
                "src/main/kotlin/example/PluginMain.kt",
                r#"package example

import org.onelastleaf.plugin.sdk.kotlin.action
import org.onelastleaf.plugin.sdk.kotlin.plugin
import org.onelastleaf.plugin.sdk.kotlin.stringResult

fun main() = plugin("__OLL_PLUGIN_ID__", "0.1.0") {
    action("echo", "Return the supplied arguments") { _, arguments ->
        stringResult(echo(arguments))
    }
}.run().toCompletableFuture().join()
"#,
            ),
            file(
                "src/test/kotlin/example/EchoTest.kt",
                "package example\n\nimport kotlin.test.Test\nimport kotlin.test.assertEquals\n\nclass EchoTest {\n    @Test fun preservesOrder() = assertEquals(\"one two\", echo(listOf(\"one\", \"two\")))\n}\n",
            ),
        ],
    )
}

pub(super) fn scala() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Scala",
            ..RECIPE
        },
        vec![
            settings(),
            file(
                "build.gradle.kts",
                r#"plugins {
    application
    scala
}

repositories { mavenCentral() }
dependencies {
    implementation("org.scala-lang:scala3-library_3:3.7.3")
    implementation("org.onelastleaf:onelastleaf-plugin-sdk-scala:0.1.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }
application { mainClass = "example.PluginMain" }
tasks.test { useJUnitPlatform() }
tasks.installDist {
    destinationDir = file(providers.gradleProperty("installDir").getOrElse("build/install"))
}
"#,
            ),
            file(
                "src/main/scala/example/Echo.scala",
                "package example\n\nobject Echo:\n  def run(arguments: Seq[String]): String = arguments.mkString(\" \")\n",
            ),
            file(
                "src/main/scala/example/PluginMain.scala",
                r#"package example

import org.onelastleaf.plugin.sdk.scala.Plugin

object PluginMain:
  def main(arguments: Array[String]): Unit =
    Plugin("__OLL_PLUGIN_ID__", "0.1.0")
      .action("echo", "Return the supplied arguments")((_, values) => Echo.run(values))
      .run()
      .toCompletableFuture
      .join()
"#,
            ),
            file(
                "src/test/scala/example/EchoTest.scala",
                "package example\n\nimport org.junit.jupiter.api.Assertions.assertEquals\nimport org.junit.jupiter.api.Test\n\nfinal class EchoTest:\n  @Test def preservesOrder(): Unit = assertEquals(\"one two\", Echo.run(Seq(\"one\", \"two\")))\n",
            ),
        ],
    )
}

pub(super) fn clojure() -> (Recipe, Vec<GeneratedFile>) {
    (
        Recipe {
            language: "Clojure",
            ..RECIPE
        },
        vec![
            settings(),
            file(
                "build.gradle.kts",
                r#"plugins {
    application
    id("dev.clojurephant.clojure") version "0.9.0"
}

repositories { mavenCentral() }
dependencies {
    implementation("org.clojure:clojure:1.12.3")
    implementation("org.onelastleaf:onelastleaf-plugin-sdk-clojure:0.1.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
application {
    mainClass = "example.PluginMain"
    applicationDefaultJvmArgs = listOf("-Dclojure.main.report=stderr")
}
tasks.test { useJUnitPlatform() }
tasks.installDist {
    destinationDir = file(providers.gradleProperty("installDir").getOrElse("build/install"))
}
"#,
            ),
            file(
                "src/main/java/example/PluginMain.java",
                "package example;\n\npublic final class PluginMain {\n    private PluginMain() {}\n    public static void main(String[] args) {\n        clojure.main.main(new String[] {\"-m\", \"example.plugin\"});\n    }\n}\n",
            ),
            file(
                "src/main/clojure/example/plugin.clj",
                r#"(ns example.plugin
  (:require [clojure.string :as string]
            [onelastleaf.plugin-sdk :as oll]))

(defn echo [arguments]
  (string/join " " arguments))

(defn echo-action [_ arguments]
  (oll/string-result (echo arguments)))

(defn -main [& _]
  (-> (oll/plugin "__OLL_PLUGIN_ID__" "0.1.0")
      (oll/action "echo" "Return the supplied arguments" echo-action)
      (oll/run)
      (.toCompletableFuture)
      (.join)))
"#,
            ),
            file(
                "src/test/java/example/EchoTest.java",
                r#"package example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentVector;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class EchoTest {
    private static IFn echo;

    @BeforeAll
    static void loadPlugin() {
        Clojure.var("clojure.core", "require").invoke(Clojure.read("example.plugin"));
        echo = Clojure.var("example.plugin", "echo");
    }

    @Test
    void preservesOrder() {
        assertEquals("one two", echo.invoke(PersistentVector.create(List.of("one", "two"))));
    }
}
"#,
            ),
        ],
    )
}
