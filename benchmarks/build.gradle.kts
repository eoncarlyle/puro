plugins {
    kotlin("jvm")
    kotlin("plugin.serialization") version "2.0.21"
    id("org.jetbrains.kotlinx.benchmark") version "0.4.11"
}

group = "com.iainschmitt.puro"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":"))
    implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.11")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
}

kotlin {
    jvmToolchain(21)
}

benchmark {
    targets {
        register("main")
    }
}
