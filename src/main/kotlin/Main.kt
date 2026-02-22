import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.LoggerFactory
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.appendBytes
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

    val segmentPath = Files.createFile(puroDirectory.resolve("stream0.puro"))
    val lowSignalBitWrite = Files.readAllBytes(Path.of("/Users/iain/code/puro/src/test/resources/incompleteSegmentLowSignalBit.puro"))

    var count = 0;

    val consumer = Consumer(
        puroDirectory,
        listOf("testTopic"),
        logger = logger,
        startPoint = ConsumerStartPoint.StreamBeginning,
        readBufferSize = 100,
    ) { record, internalLogger ->
        count++;
        internalLogger.info("${String(record.topic)}/${String(record.key.array())}/${String(record.value.array())}")
    }
    consumer.run()

    val producer = Producer(puroDirectory, 10, 100)

    val firstValue = """
            All fines that have been given to us unjustly and against the law of the land, and all fines that we have exacted
            unjustly, shall be entirely remitted or the matter decided by a majority judgment of the twenty-five barons referred to
            below in the clause for securing the peace (§61) together with Stephen, archbishop of Canterbury, if he can be present,
            and such others as he wishes to bring with him. If the archbishop cannot be present, proceedings shall continue without
            him, provided that if any of the twenty-five barons has been involved in a similar suit himself, his judgment shall be
            set aside, and someone else chosen and sworn in his place, as a substitute for the single occasion, by the rest of the
            twenty-five.
            """.trimIndent().replace("\n", "")

    val secondValue = """
            Earls and barons shall be fined only by their equals, and in proportion to the gravity of their offence.
            """.trimIndent().replace("\n", "")

    segmentPath.appendBytes(lowSignalBitWrite)

    producer.send(
        listOf(
            PuroRecord("testTopic", "testKey".toByteBuffer(), firstValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), secondValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), "TrueProducerSmallValue".toByteBuffer())
        )
    )

    Thread.sleep(100)

    // Suspicious and frankly strange timing here, not sure what the deal with that is
    //Thread.sleep(100)
    segmentPath.appendBytes(lowSignalBitWrite)
    Thread.sleep(200)
}