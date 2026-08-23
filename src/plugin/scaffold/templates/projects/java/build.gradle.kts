plugins {
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
