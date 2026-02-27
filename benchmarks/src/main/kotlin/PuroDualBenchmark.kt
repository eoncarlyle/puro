package com.iainschmitt.puro

import Consumer
import PuroRecord
import Producer
import org.openjdk.jmh.annotations.*
import org.slf4j.helpers.NOPLogger
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.io.path.exists

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
open class PuroDualBenchmark {

    private lateinit var producer: Producer
    private lateinit var consumer: Consumer
    private lateinit var records: List<PuroRecord>
    private var streamDirectory: Path = Files.createTempDirectory(System.currentTimeMillis().toString())
    private var measurementFactory = WeatherMeasurementFactory()
    private val measurements = 1_000_000L;
    private var count = 0L
    private var averageMap = HashMap<String, Pair<Int, Double>>()
    private var semaphore = Semaphore(1, true)

    @Setup(Level.Trial)
    fun setup() {
        records = measurementFactory.getMeasurements(measurements)
    }

    @Setup(Level.Invocation)
    fun setupInvocation() {
        count = 0L
        averageMap.clear()
        producer = Producer(streamDirectory, 8192)
        semaphore = Semaphore(1, true)
        semaphore.acquire()
        consumer = Consumer(streamDirectory, listOf("temperatures"), logger = NOPLogger.NOP_LOGGER) { record, _ ->
            onMessage(record)
        }
    }

    @TearDown(Level.Invocation)
    fun tearDownInvocation() {
        consumer.stopListening()
        Files.deleteIfExists(streamDirectory.resolve("stream0.puro"))
    }

    @Benchmark
    fun run() {
        producer.send(records)
        consumer.run()
        semaphore.acquire()
    }

    @TearDown
    fun tearDown() {
        if (streamDirectory.exists()) {
            streamDirectory.toFile().deleteRecursively()
        }
    }

    private fun onMessage(record: PuroRecord) {
        val stationName = String(record.key.array())
        val measurement = record.value.getDouble()
        val averageKey = averageMap[stationName]

        if (averageKey != null) {
            val (measurementCount, average) = averageKey
            val newCount = averageKey.first + 1
            val newAverage = (measurement + average * measurementCount) / newCount
            averageMap[stationName] = newCount to newAverage
        } else {
            averageMap[stationName] = 1 to measurement
        }
        count++

        if (count == measurements) {
            semaphore.release()
        }
    }
}
