import PuroProducer.Companion.retryDelay
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun getRecord(byteBuffer: ByteBuffer): PuroRecord? {
    if (!byteBuffer.hasRemaining()) {
        return null //Actual result type
    }

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
    } else null //Actual result type
}

private fun allNonNull(
    lengthData: Triple<Int, Int, Byte>?,
    topicLengthData: Triple<Int, Int, Byte>?,
    keyMetadata: Triple<Int, Int, Byte>?,
) = lengthData != null && topicLengthData != null && keyMetadata != null


fun getFetchInteriorRecords(byteBuffer: ByteBuffer, initialOffset: Long): Pair<List<PuroRecord>, Long> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset

    while (byteBuffer.hasRemaining()) {
        val expectedCrc = byteBuffer.get()

        // The naming needs to be cleaned up: this isn't easy to follow

        //val (encodedTotalLength, encodedTotalLengthBitCount, crc1) = byteBuffer.fromVlq()
        val lengthData = byteBuffer.readSafety()?.fromVlq()

        //val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
        val topicLengthData = byteBuffer.readSafety()?.fromVlq()

        //val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
        val topicMetadata = if (topicLengthData != null) {
            byteBuffer.readSafety()?.getEncodedString(topicLengthData.first)
        } else null

        //val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
        val keyMetdata = byteBuffer.readSafety()?.fromVlq()

        // val (key, crc5) = byteBuffer.getSubsequence(keyLength)
        val keyData = if (keyMetdata != null) {
            byteBuffer.readSafety()?.getSubsequence(keyMetdata.first)
        } else null

        // val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)
        val valueData = if (lengthData != null && topicLengthData != null && keyMetdata != null) {
            byteBuffer.readSafety()
                ?.getSubsequence(lengthData.first - topicLengthData.second - topicLengthData.first - keyMetdata.second - keyMetdata.first)
        } else null

        // The else branch isn't advancing the offset because it is possible that this is the next batch
        if (lengthData != null && topicLengthData != null && topicMetadata != null && keyMetdata != null && keyData != null && valueData != null) {
            val (_, encodedTotalLengthBitCount, crc1) = lengthData
            val (topicLength, topicLengthBitCount, crc2) = topicLengthData
            val (topic, crc3) = topicMetadata
            val (keyLength, keyLengthBitCount, crc4) = keyMetdata
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

            val subrecordLength = topicLengthBitCount + topicLength + keyLengthBitCount + keyLength + value.capacity()
            val totalLength = 1 + encodedTotalLengthBitCount + subrecordLength

            if (expectedCrc == actualCrc) {
                records.add(PuroRecord(topic, key, value))
            }

            // These are necessarily 'interior' messages.
            offset += totalLength
        }
    }
    return Pair(records, offset)
}

class PuroConsumer(
    val streamDirectory: Path,
    val topics: List<String>,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) {
    var activeSegmentPath = getActiveSegmentPath(streamDirectory)
    var consumerOffset = 0L //First byte offset that hasn't been read

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    if (event.path() == activeSegmentPath) {
                        val producerOffset = Files.size(activeSegmentPath)
                        withConsumerLock(consumerOffset, producerOffset - consumerOffset) {

                            // TODO: need to do this in testable way
                            // If we will never get events with events that will cross DirectoryChangeEvent
                            // boundaries then that makes this much simpler but I don't know how realistic
                            // that is. If we can't assume that then we'd have to store the truncated message
                            // fragment alongside the segment offset and then concatenate it with the new poll
                            // however, telling the difference between a truncated message that will be
                            // be completed from a permanently errored message may not be really possible
                        }
                    }
                }

                DirectoryChangeEvent.EventType.CREATE -> {} //May have active segment management concerns
                DirectoryChangeEvent.EventType.DELETE -> {} //Log something probably
                DirectoryChangeEvent.EventType.OVERFLOW -> {} //Log something probably
                else -> {} // This should never happen
            }
        }
        .build()

    fun stopListening() = watcher?.close()

    fun listen(): Thread {
        return Thread { watcher?.watch() }
    }

    fun onActiveSegmentModifyEvent(producerOffset: Long) {

    }

    private fun getActiveSegmentChannel(): FileChannel {
        // Setting this as a side effect, not super jazzed about this
        activeSegmentPath = getActiveSegmentPath(streamDirectory)
        return FileChannel.open(activeSegmentPath, StandardOpenOption.READ)
    }

    private fun withConsumerLock(position: Long, size: Long, block: (FileChannel) -> Unit) {
        // There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
        // the active segment could change.
        // This has been marked on the README as 'Active segment transition race condition handling'
        // Best way may be to read if 'tombstone'
        getActiveSegmentChannel().use { channel ->
            var lock: FileLock?
            do {
                lock = channel.tryLock(position, size, true)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            block(channel)
        }
    }
}
