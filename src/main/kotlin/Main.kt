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
    val initialProducer = Producer(puroDirectory, 10, 100, logger)

    val segmentPath = Files.createFile(puroDirectory.resolve("stream0.puro"))
    val lowSignalBitWrite = Files.readAllBytes(Path.of("/Users/iain/code/puro/reference/completeSegment.puro"))

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
    /*
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
     */
    //segmentPath.appendBytes(lowSignalBitWrite)


    val firstValue = """
    No free man shall be seized or imprisoned, or stripped of his rights or possessions, or outlawed or exiled, or
    deprived of his standing in any way, nor will we proceed with force against him, or send others to do so, except by
    the lawful judgment of his equals or by the law of the land."
    """.trimIndent().replace("\n", "")

    val secondValue = """
    All merchants may enter or leave England unharmed and without fear, and may stay or travel within it, by land 
    or water, for purposes of trade, free from all illegal exactions, in accordance with ancient and lawful customs. 
    This, however, does not apply in time of war to merchants from a country that is at war with us. Any such 
    merchants found in our country at the outbreak of war shall be detained without injury to their persons or 
    property, until we or our chief justice have discovered how our own merchants are being treated in the country 
    at war with us. If our own merchants are safe they shall be safe too. 
    """.trimIndent().replace("\n", "")

    val thirdValue = """
    To no one will we sell, to no one deny or delay right or justice. 
    """.trimIndent().replace("\n", "")

    initialProducer.send(
        listOf(
            PuroRecord("testTopic", "testKey".toByteBuffer(), firstValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), secondValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), thirdValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), "SmallValue".toByteBuffer())
        )
    )

    Thread.sleep(100)

    // Suspicious and frankly strange timing here, not sure what the deal with that is
    Thread.sleep(100)
    //segmentPath.appendBytes(lowSignalBitWrite)
    Thread.sleep(200)
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