import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration
import kotlin.math.min

private data class SerialisedPuroRecord(
    val messageCrc: Byte,
    val encodedTotalLength: ByteBuffer,
    val encodedTopicLength: ByteBuffer,
    val encodedTopic: ByteArray,
    val encodedKeyLength: ByteBuffer,
    val key: ByteBuffer,
    val value: ByteBuffer
)

fun rewindAll(vararg bytes: ByteBuffer) = bytes.forEach { it.rewind() }

fun createRecordBuffer(record: PuroRecord): ByteBuffer {
    val (topic, key, value) = record
    rewindAll(key, value)

    // When I finally get around to changing the questionable variable names, change this for `onHardTransitionCleanup` too
    // Should have the lengths, CRCs ready to go - should hold the lock for as short a time as possible
    val topicLength = topic.size
    val encodedTopicLength = topicLength.toVlqEncoding()
    val keyLength = key.capacity()
    val encodedKeyLength = keyLength.toVlqEncoding()
    val valueLength = value.capacity()

    val subrecordLength =
        encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
    val encodedTotalLength = subrecordLength.toVlqEncoding()

    // crc8 + totalLength + (topicLength + topic + keyLength + key + value)
    val recordBuffer = ByteBuffer.allocate(RECORD_CRC_BYES + encodedTotalLength.capacity() + subrecordLength)

    val messageCrc = getMessageCrc(
        encodedTotalLength = encodedTotalLength,
        encodedTopicLength = encodedTopicLength,
        topic = topic,
        encodedKeyLength = encodedKeyLength,
        key = key,
        value = value
    )
    rewindAll(encodedTotalLength, encodedTopicLength, encodedKeyLength, key, value)

    recordBuffer.put(messageCrc).put(encodedTotalLength).put(encodedTopicLength).put(topic)
        .put(encodedKeyLength).put(key).put(value)

    return recordBuffer.rewind()
}

fun createBatchedRecordBuffer(puroRecords: List<PuroRecord>): ByteBuffer {
    val protoRecords = Array<SerialisedPuroRecord?>(puroRecords.size) { null }
    var batchLength = 0

    puroRecords.forEachIndexed { index, record ->
        val (topic, key, value) = record
        rewindAll(key, value)

        // Should have the lengths, CRCs ready to go - should hold the lock for as short a time as possible
        val topicLength = topic.size
        val encodedTopicLength = topicLength.toVlqEncoding()
        val keyLength = key.capacity()
        val encodedKeyLength = keyLength.toVlqEncoding()
        val valueLength = value.capacity()

        val subrecordLength =
            encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
        val encodedTotalLength = subrecordLength.toVlqEncoding()

        // crc8 + totalLength + (topicLength + topic + keyLength + key + value)
        val recordLength = RECORD_CRC_BYES + encodedTotalLength.capacity() + subrecordLength
        batchLength += recordLength

        val messageCrc = getMessageCrc(
            encodedTotalLength = encodedTotalLength,
            encodedTopicLength = encodedTopicLength,
            topic = topic,
            encodedKeyLength = encodedKeyLength,
            key = key,
            value = value
        )

        rewindAll(encodedTotalLength, encodedTopicLength, encodedKeyLength, key, value)

        protoRecords[index] = SerialisedPuroRecord(
            messageCrc,
            encodedTotalLength,
            encodedTopicLength,
            topic,
            encodedKeyLength,
            key,
            value
        )
    }

    val batchBuffer = ByteBuffer.allocate(batchLength)

    protoRecords.forEach { record: SerialisedPuroRecord? ->

        val (messageCrc,
            encodedTotalLength,
            encodedTopicLength,
            encodedTopic,
            encodedKeyLength,
            key,
            value) = record!!

        batchBuffer.put(messageCrc).put(encodedTotalLength).put(encodedTopicLength).put(encodedTopic)
            .put(encodedKeyLength).put(key).put(value)
    }

    return batchBuffer.rewind()
}

fun getMessageCrc(
    encodedTotalLength: ByteBuffer,
    encodedTopicLength: ByteBuffer,
    topic: ByteArray,
    encodedKeyLength: ByteBuffer,
    key: ByteBuffer,
    value: ByteBuffer,
): Byte =
    crc8(encodedTotalLength) //TODO get on same page about using buffers here - concerned of `ByteBuffer#array` cost
        .withCrc8(encodedTopicLength)
        .withCrc8(topic)
        .withCrc8(encodedKeyLength)
        .withCrc8(key)
        .withCrc8(value)

//  is the number of messages to be sent before reliquishing the lock
// This should have a maximum value to prevent starvation
// Probably better to return null/failing result for misconfiguration
class PuroProducer(
    val streamDirectory: Path,
    val maximumWriteBatchSize: Int,
) {
    companion object {
        // This should be configurable?
        private val retryDelay = 10.milliseconds.toJavaDuration()
    }

    fun send(puroRecord: PuroRecord) {
        val recordBuffer = createRecordBuffer(puroRecord)
        withProducerLock { channel -> channel.write(recordBuffer) }
    }

    fun getStepCount(puroRecords: List<PuroRecord>) = if (puroRecords.size % maximumWriteBatchSize == 0) {
        puroRecords.size / maximumWriteBatchSize // 49/7 = 7
    } else {
        (puroRecords.size / maximumWriteBatchSize) + 1 // maximum modulo is one smaller than the batch
    }

    fun sendUnbatched(
        puroRecords: List<PuroRecord>,
        onRecord: (List<ByteBuffer>) -> (FileChannel) -> Unit
    ) {
        val stepCount = getStepCount(puroRecords)

        for (step in 0..<stepCount) {
            val indices = step * maximumWriteBatchSize..<min((step + 1) * maximumWriteBatchSize, puroRecords.size)
            val recordBuffers = puroRecords.slice(indices).map { createRecordBuffer(it) }
            withProducerLock { channel ->
                val inner = onRecord(recordBuffers)
                inner(channel)
            }
        }
    }

    fun sendBatched(puroRecords: List<PuroRecord>, onBatch: (ByteBuffer) -> (FileChannel) -> Unit) {
        val stepCount = getStepCount(puroRecords)

        for (step in 0..<stepCount) {
            val indices = step * maximumWriteBatchSize..<min((step + 1) * maximumWriteBatchSize, puroRecords.size)
            val batchedBuffer = createBatchedRecordBuffer(puroRecords.slice(indices))
            withProducerLock { channel ->
                val fn = onBatch(batchedBuffer)
                fn(channel)
            }
        }
    }

    fun send(puroRecords: List<PuroRecord>, isBatched: Boolean = true) {
        if (isBatched) {
            sendBatched(puroRecords) { buffer -> { channel -> channel.write(buffer) } }
        } else {
            sendUnbatched(puroRecords) { buffers -> { channel -> buffers.forEach { channel.write(it) } } }
        }
    }

    fun withProducerLock(block: (FileChannel) -> Unit) {
        // There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
        // the active segment could change.
        // This has been marked on the README as 'Active segment transition race condition handling'
        // Best way may be to read if 'tombstone'
        FileChannel.open(getActiveSegment(streamDirectory, true), StandardOpenOption.APPEND).use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            block(channel)
        }
    }
}
