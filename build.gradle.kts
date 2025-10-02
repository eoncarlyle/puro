plugins {
    kotlin("jvm") version "2.0.21"
}

group = "com.iainschmitt.puro"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.methvin:directory-watcher:0.19.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
