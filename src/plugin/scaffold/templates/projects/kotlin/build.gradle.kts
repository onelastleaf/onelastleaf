plugins {
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
