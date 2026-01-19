import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
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
    val logger = LoggerFactory.getLogger("MainKt")

    Path("/tmp/puro/segment0.puro").deleteIfExists()
    val producer = SignalBitProducer(Path("/tmp/puro"), 10)

    val longerKey = """
    No free man shall be seized or imprisoned, or stripped of his rights or possessions, or outlawed or exiled, or
    deprived of his standing in any way, nor will we proceed with force against him, or send others to do so, except by 
    the lawful judgment of his equals or by the law of the land."
    """.trimIndent()

    producer.send(listOf(PuroRecord("testTopic", "testKey".toByteBuffer(), longerKey.toByteBuffer())))

    val consumer = PuroConsumer(
        Path("/tmp/puro"),
        listOf("testTopic"),
        logger,
        startPoint = ConsumerStartPoint.StreamBeginning,
        readBufferSize = 100
    ) { record, internalLogger ->
        internalLogger.info("${String(record.topic)}/${String(record.key.array())}/${String(record.value.array())}")
    }
    consumer.run()
}

/*
fun main() {
    Path("/tmp/puro/segment0.puro").deleteIfExists()
    val producer = LegacyPuroProducer(Path("/tmp/puro"), 10)
    val logger = LoggerFactory.getLogger("MainKt")
    val consumer = PuroConsumer(Path("/tmp/puro"), listOf("testTopic"), logger, startPoint = ConsumerStartPoint.StreamBeginning, readBufferSize = 100) {
        println("${String(it.topic)}/${String(it.key.array())}/${String(it.value.array())}")
    }
    consumer.run()
    //Thread.sleep(1)
    var increment = 0
    repeat(1) {
        val messages = (0..<1).map {
            increment++
            PuroRecord(
                "testTopic", "key${increment}".toByteBuffer(),
                ("value${(0..<250).map { "_" }.joinToString { "" } }" + "!").toByteBuffer()
            )
        }
        logger.info("Sending batch")
        producer.send(messages)
        //Thread.sleep(100)
    }
}

*/