import PuroProducer.Companion.retryDelay
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun getRecord(byteBuffer: ByteBuffer): Pair<PuroRecord, Int>? {
    if (!byteBuffer.hasRemaining()) {
        return null //Actual result type
    }
    val start = byteBuffer.position()
    val expectedCrc = byteBuffer.get()
    val (encodedTotalLength, _, crc1) = byteBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
    val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
    val (key, crc5) = byteBuffer.getSubsequence(keyLength)
    val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        PuroRecord(topic, key, value) to (byteBuffer.position() - start)
    } else null //TODO Actual result type
}

fun getRecords(
    byteBuffer: ByteBuffer,
    initialOffset: Long,
    isEndOfFetch: Boolean = false
): Triple<List<PuroRecord>, Long, Boolean> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var abnormality = false // Only matters if end-of-fetch

    while (byteBuffer.hasRemaining()) {
        val expectedCrc = byteBuffer.get()

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
        } else {
            abnormality = true
        }
    }
    return Triple(records, offset, if (isEndOfFetch) abnormality else false)
}

class PuroConsumer(
    val streamDirectory: Path,
    val topics: List<String>,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) {
    companion object {
        const val BUFFSIZE = 8192
    }

    var activeSegmentPath = getActiveSegmentPath(streamDirectory)
    var consumerOffset = 0L //First byte offset that hasn't been read
    val readBuffer = ByteBuffer.allocate(BUFFSIZE)

    var abnormalOffsetWindow: Pair<Long, Long>? = null

    fun stopListening() = watcher?.close()

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    if (event.path() == activeSegmentPath) {
                        onActiveSegmentChange()
                    }
                }

                DirectoryChangeEvent.EventType.CREATE -> {} //May have active segment management concerns
                DirectoryChangeEvent.EventType.DELETE -> {} //Log something probably
                DirectoryChangeEvent.EventType.OVERFLOW -> {} //Log something probably
                else -> {} // This should never happen
            }
        }
        .build()


    fun onActiveSegmentChange() {
        val producerOffset = Files.size(activeSegmentPath)
        val offsetChange = producerOffset - consumerOffset
        val records = ArrayList<PuroRecord>()

        val exitCode = withConsumerLock(consumerOffset, offsetChange) { fileChannel ->

            if (abnormalOffsetWindow == null) {
                standardRead(fileChannel, records, producerOffset, offsetChange)
            } else {
                readBuffer.clear()
                fileChannel.read(readBuffer)
                readBuffer.flip()
                // Continuation: clean message divide over a fetch boundary
                val continuationResult = getRecord(readBuffer)
                if (continuationResult != null) {
                    consumerOffset += continuationResult.second
                    abnormalOffsetWindow = null
                    val newOffset = producerOffset - consumerOffset
                    standardRead(fileChannel, records, producerOffset, newOffset)
                    // No cleanup required
                    return@withConsumerLock 0
                } else {
                    // Hard producer transition: last producer failed, but new messages not interrupted
                    readBuffer.rewind()
                    // Off-by-one possibility
                    // TODO buffer saftey: integer conversions may be an issue
                    val bufferOffset = (abnormalOffsetWindow!!.second - abnormalOffsetWindow!!.first).toInt()
                    readBuffer.position(bufferOffset + 1)

                    val hardProducerTransitionResult = getRecord(readBuffer)

                    if (hardProducerTransitionResult != null) {
                        consumerOffset += hardProducerTransitionResult.second
                        abnormalOffsetWindow = null
                        // TODO: replace with result type
                        // The consumer can fix the segment but it needs to relinquish it's shared lock first
                        return@withConsumerLock 1
                    } else {
                        // Failed producer transition
                        // TODO: replace with result type
                        return@withConsumerLock -1
                    }
                }
            }
        }

        // TODO: result type handling
        if (exitCode == 0) {
            records.filter { topics.contains(it.topic) }.forEach { onMessage(it) }
        } else if (exitCode == 1) {
            onHardProducerTransition()
        } else {
            throw RuntimeException("Unexpected exit code $exitCode")
        }
    }

    fun onHardProducerTransition() {
        //TODO: Write a message with a zero'd CRC bit, zero topic and key lengths, with a message length to match the gap
        //TODO: Re-acquire a read lock and continue on as normal
    }

    fun getStepCount(offsetDelta: Long) = (if (offsetDelta % BUFFSIZE == 0L) {
        (offsetDelta / BUFFSIZE)
    } else {
        (offsetDelta / BUFFSIZE) + 1
    }).toInt()


    fun listen(): Thread {
        return Thread { watcher?.watch() }
    }

    fun standardRead(fileChannel: FileChannel, records: ArrayList<PuroRecord>, producerOffset: Long, offsetChange: Long) {
        val steps = getStepCount(offsetChange)
        for (step in 0..<steps) {
            readBuffer.clear()
            fileChannel.read(readBuffer)
            readBuffer.flip()
            val isLastBatch = (step == steps - 1)
            val (batchRecords, nextOffset, abnormality) = getRecords(
                readBuffer,
                consumerOffset,
                isEndOfFetch = isLastBatch
            )

            records.addAll(batchRecords)
            consumerOffset = nextOffset

            if (isLastBatch && abnormality) {
                abnormalOffsetWindow = consumerOffset to producerOffset
            }
        }
    }

    private fun getActiveSegmentChannel(): FileChannel {
        // Setting this as a side effect, not super jazzed about this
        activeSegmentPath = getActiveSegmentPath(streamDirectory)
        return FileChannel.open(activeSegmentPath, StandardOpenOption.READ)
    }

    // Consumer lock block: returns `false` if end-of-fetch abnormality
    private fun <T> withConsumerLock(position: Long, size: Long, block: (FileChannel) -> T): T {
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
            return block(channel)
        }
    }
}
