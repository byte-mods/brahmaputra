// Gradle build for the Kotlin wrapper. The Java driver is compiled from ../java as part of
// this module's main source set (Kotlin reads its sources for types, javac compiles them),
// so the jar carries both and nothing is copied.
//
//   gradle build                       # build/libs/brahmaputra-kotlin-0.1.0.jar
//   gradle manualTest -Pbroker=127.0.0.1:9092
//

plugins {
    kotlin("jvm") version "2.4.20"
    `java-library`
}

group = "io.brahmaputra"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
}

// Target Java 17 bytecode from whatever JDK (17+) runs Gradle, without requiring a
// separately installed 17 toolchain.
kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        allWarningsAsErrors.set(true)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

sourceSets {
    main {
        java.srcDir("../java/src/main/java")
    }
}

// The end-to-end suite needs a live broker, so it is a run task rather than a unit test.
tasks.register<JavaExec>("manualTest") {
    group = "verification"
    description = "Runs the 54-check end-to-end suite against -Pbroker=HOST:PORT"
    dependsOn("testClasses")
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("io.brahmaputra.kt.ManualTestKt")
    val broker = (project.findProperty("broker") as String?) ?: "127.0.0.1:9092"
    args(broker.substringBeforeLast(':'), broker.substringAfterLast(':'))
}
