import PuroProducer.Companion.retryDelay
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun getRecord(byteBuffer: ByteBuffer): PuroRecord? {
    val expectedCrc = byteBuffer.get()
    val (encodedTotalLength, _, crc1) = byteBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
    val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
    val (key, crc5) = byteBuffer.getSubsequence(keyLength)
    val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        PuroRecord(topic, key, value)
    } else null
}


class PuroConsumer(
    val streamDirectory: Path,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) {
    var activeSegmentPath = getActiveSegment(streamDirectory)
    var activeSegmentChannel = FileChannel.open(activeSegmentPath, StandardOpenOption.READ)
    var offset = 0

    val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    if (event.path() == activeSegmentPath) {
                        withConsumerLock {
                            // TODO: need to do this in testable way
                            // If we will never get events with events that will cross DirectoryChangeEvent
                            // boundaries then that makes this much simpler but I don't know how realistic
                            // that is. If we can't assume that then we'd have to store the truncated message
                            // fragment alongside the segment offset and then concatenate it with the new poll
														// however, telling the difference between a truncated message that will be
                            // be completed from a permanently errored message may not be really possible
                        }
                    } // else {} //Active Segment concerns
                }

                DirectoryChangeEvent.EventType.CREATE -> {} //May have active segment management concerns
                DirectoryChangeEvent.EventType.DELETE -> {} //Log something probably
                DirectoryChangeEvent.EventType.OVERFLOW -> {} //Log something probably
            }
        }
        .build()

    fun stopListening() = watcher?.close()

    fun listen(): Thread {
        return Thread { watcher?.watch() }
    }

    private fun updateActiveSegment() {
        activeSegmentPath = getActiveSegment(streamDirectory)
        activeSegmentChannel = FileChannel.open(activeSegmentPath)
    }



    fun withConsumerLock(block: (FileChannel) -> Unit) {
        withLock(activeSegmentChannel, block)
    }
}
