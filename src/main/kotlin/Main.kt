import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
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

    //Path("/tmp/puro/stream0.puro").deleteIfExists()
    val initialProducer = SignalBitProducer(Path("/tmp/puro"), 10, 100, logger)

    //val firstValue = """
    //No free man shall be seized or imprisoned, or stripped of his rights or possessions, or outlawed or exiled, or
    //deprived of his standing in any way, nor will we proceed with force against him, or send others to do so, except by
    //the lawful judgment of his equals or by the law of the land."
    //""".trimIndent().replace("\n", "")

    //initialProducer.send(listOf(PuroRecord("testTopic", "testKey".toByteBuffer(), firstValue.toByteBuffer())))

    //val consumer = PuroConsumer(
    //    Path("/tmp/puro"),
    //    listOf("testTopic"),
    //    logger,
    //    startPoint = ConsumerStartPoint.StreamBeginning,
    //    readBufferSize = 100
    //) { record, internalLogger ->
    //    internalLogger.info("${String(record.topic)}/${String(record.key.array())}/${String(record.value.array())}")
    //}
    //consumer.run()

    //Thread.sleep(1000)
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
            PuroRecord("testTopic", "testKey".toByteBuffer(), secondValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), thirdValue.toByteBuffer()),
            PuroRecord("testTopic", "testKey".toByteBuffer(), "SmallValue".toByteBuffer())
        )
    )
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