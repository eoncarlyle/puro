package com.iainschmitt.puro

import PuroProducer
import PuroRecord
import org.openjdk.jmh.annotations.*
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class PuroProducerBenchmark {

    private lateinit var producer: PuroProducer
    private lateinit var records: List<PuroRecord>

    @Setup
    fun setup() {
        producer = PuroProducer("/tmp/puro", 100)

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
    fun benchmarkSend() {
        producer.send(records)
    }

    @TearDown
    fun tearDown() {
    }
}
