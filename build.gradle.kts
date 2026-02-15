plugins {
    kotlin("jvm") version "2.0.21"
}

group = "com.iainschmitt.puro"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

configurations.testRuntimeClasspath {
    exclude(group = "org.slf4j", module = "slf4j-simple")
}

dependencies {
    implementation("io.methvin:directory-watcher:0.19.1")
    implementation("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.9") {
        because("Excluded from tests where slf4j-test is the binding")
    }
    testImplementation(kotlin("test"))
    testImplementation("com.github.valfirst:slf4j-test:3.0.3")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
