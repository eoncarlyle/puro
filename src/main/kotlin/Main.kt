import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import org.slf4j.helpers.NOPLogger
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Semaphore
import kotlin.io.path.Path
import kotlin.io.path.appendBytes
import kotlin.io.path.createFile
import kotlin.io.path.deleteIfExists

class DirectoryWatchingUtility(directoryToWatch: Path?, onEvent: (DirectoryChangeEvent) -> Unit) {
    val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(directoryToWatch) // or use paths(directoriesToWatch)
        .listener {
            onEvent(it)
        } // .fileHashing(false) // defaults to true
        // .logger(logger) // defaults to LoggerFactory.getLogger(DirectoryWatcher.class)
        // .watchService(watchService) // defaults based on OS to either JVM WatchService or the JNA macOS WatchService
        .build()

    fun stopWatching() {
        watcher?.close()
    }
}

fun directory() {
    val path = Path("/tmp/puro/stream0.puro")
    val channel = FileChannel.open(path)

    val onEvent: (DirectoryChangeEvent) -> Unit = { event: DirectoryChangeEvent ->
        when (event.eventType()) {
            DirectoryChangeEvent.EventType.MODIFY -> {
                if (path == event.path()) {
                    println(channel.size())
                }
            }

            else -> println(event)
        }
    }

    DirectoryWatchingUtility(Path("/tmp/puro"), onEvent).watcher?.watch()
}


fun main() {
    val logger = LoggerFactory.getLogger("MainKt")
    Path("/tmp/puro/stream0.puro").deleteIfExists()
    val puroDirectory = Path("/tmp/puro")
    Path.of("/tmp/puro/stream0.puro").createFile()

    val producer = Producer(puroDirectory, 8192)

    val measurementFactory = MeasurementFactory()
    val measurements = 1_000_000L;
    var count = 0L
    val averageMap = HashMap<String, Pair<Int, Double>>()
    val semaphore = Semaphore(1, true)

    val consumer = Consumer(puroDirectory, listOf("temperatures"), logger = NOPLogger.NOP_LOGGER) { record, _ ->
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

    val records = measurementFactory.getMeasurements(measurements)

    consumer.run()
    producer.send(records)
    semaphore.acquire()
}