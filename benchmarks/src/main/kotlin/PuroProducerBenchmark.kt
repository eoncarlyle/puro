package com.iainschmitt.puro

import PuroProducer
import PuroRecord
import org.openjdk.jmh.annotations.*
import kotlinx.serialization.json.Json
import org.openjdk.jmh.infra.Blackhole
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.io.path.Path
import kotlin.io.path.exists

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class PuroProducerBenchmark {

    private lateinit var producer: PuroProducer
    private lateinit var records: List<PuroRecord>
    var streamDirectory = Path("/tmp/puro")

    @Setup
    fun setup() {
        resetDirectory()
        Files.createDirectory(streamDirectory)
        producer = PuroProducer(streamDirectory, 100)

        val flagsJson = object {}.javaClass.getResourceAsStream("/flags.json")
            ?.bufferedReader()
            ?.readText() ?: ""

        records = Json.decodeFromString<Map<String, FlagData>>(flagsJson).map { (key, value) ->
            PuroRecord(
                "flags",
                ByteBuffer.wrap(key.toByteArray()),
                ByteBuffer.wrap(value.toString().toByteArray())
            )
        }.toList()
    }

    @Benchmark
    fun sendBatched() {
        producer.send(records)
    }

    @Benchmark
    fun sendUnbatched() {
        producer.send(records, false)
    }

    @Benchmark
    fun serialiseUnbatched(blackhole: Blackhole) {
        producer.sendUnbatched(records) { buffers ->
            { _ -> blackhole.consume(buffers) }
        }
    }

    @Benchmark
    fun serialiseBatched(blackhole: Blackhole) {
        producer.sendBatched(puroRecords = records) { buffer ->
            { _ -> blackhole.consume(buffer) }
        }
    }

    @TearDown
    fun tearDown() {
        resetDirectory()
    }

    private fun resetDirectory() {
        if (streamDirectory.exists()) {
            Files.walk(streamDirectory)
                .sorted(Comparator.reverseOrder())
                .filter { it != streamDirectory }
                .forEach { Files.deleteIfExists(it) }
            Files.deleteIfExists(streamDirectory)
        }
    }
}
