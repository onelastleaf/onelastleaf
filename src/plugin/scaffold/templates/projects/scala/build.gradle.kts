plugins {
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
