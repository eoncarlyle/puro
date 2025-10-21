import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file.Path
import kotlin.io.path.Path
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
    Path("/tmp/puro/segment0.puro").deleteIfExists()
    val producer = PuroProducer(Path("/tmp/puro"), 10)
    val logger = LoggerFactory.getLogger("MainKt")
    val consumer = PuroConsumer(Path("/tmp/puro"), listOf("testTopic"), logger) {
        val buffer = it.value
        val byteArray = ByteArray(buffer.remaining()).apply {
            buffer.get(this)
        }
        logger.info (String(byteArray, Charset.defaultCharset()))
    }
    consumer.run()
    var a = 0
    repeat(5) {
        val messages = (0..<10).map {
            a++
            PuroRecord(
                "testTopic", a.toVlqEncoding(),
                a.toString().toByteBuffer()
            )
        }
        producer.send(messages)
        Thread.sleep(1000)
    }
}

