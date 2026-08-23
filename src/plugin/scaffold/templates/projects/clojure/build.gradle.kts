plugins {
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
