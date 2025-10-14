import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
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
    val producer = PuroProducer(Path("/tmp/puro"), 10)
    val consumer = PuroConsumer(Path("/tmp/puro"), listOf("testTopic"), LoggerFactory.getLogger("MainKt")) {
        //println(Charsets.UTF_8.decode(it.value).toString())
    }
    consumer.run()
    Path("/tmp/puro/stream0.puro").deleteIfExists()
    repeat(5) {
        val messages = (0..<10).map {
            PuroRecord(
                "testTopic", ByteBuffer.wrap(it.toString().toByteArray()),
                ByteBuffer.wrap(it.toString().hashCode().toString().toByteArray())
            )
        }
        producer.send(messages)
        Thread.sleep(1000)
    }
}

