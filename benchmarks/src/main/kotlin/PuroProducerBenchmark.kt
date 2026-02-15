package com.iainschmitt.puro

import PuroRecord
import Producer
import org.openjdk.jmh.annotations.*
import kotlinx.serialization.json.Json
import toByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.exists

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class PuroProducerBenchmark {

    private lateinit var producer: Producer
    private lateinit var records: List<PuroRecord>
    private var streamDirectory: Path = Files.createTempDirectory(System.currentTimeMillis().toString())

    @Setup
    fun setup() {
        producer = Producer(streamDirectory, 8192)

        val flagsJson = object {}.javaClass.getResourceAsStream("/flags.json")
            ?.bufferedReader()
            ?.readText() ?: ""

        records = Json.decodeFromString<Map<String, FlagData>>(flagsJson).map { (key, value) ->
            PuroRecord(
                "flags",
                key.toByteBuffer(),
                value.toString().toByteBuffer(),
            )
        }.toList()
    }

    @Benchmark
    fun sendBatched() {
        producer.send(records)
    }


    @TearDown
    fun tearDown() {
        if (streamDirectory.exists()) {
            streamDirectory.toFile().deleteRecursively()
        }
    }
}
