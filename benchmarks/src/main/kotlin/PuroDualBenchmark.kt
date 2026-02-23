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
    private var measurementFactory = MeasurementFactory()
    private var measurements = 1_000_000L;
    private var count = 0L
    private var averageMap = HashMap<String, Pair<Int, Double>>()
    private var semaphore = Semaphore(1, true)

    @Setup
    fun setup() {
        producer = Producer(streamDirectory, 8192)

        consumer = Consumer(streamDirectory, listOf("temperatures"), logger = NOPLogger.NOP_LOGGER) { record, _ ->
            semaphore.acquire()
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

            if (count == measurements + 1L) {
                semaphore.release()
            }
        }

        records = measurementFactory.getMeasurements(measurements)
    }

    @Benchmark
    fun run() {
        count = 0
        averageMap = HashMap()

       consumer.run()
        producer.send(records)
        semaphore.acquire()
    }

    @TearDown
    fun tearDown() {
        if (streamDirectory.exists()) {
            streamDirectory.toFile().deleteRecursively()
        }
    }
}
