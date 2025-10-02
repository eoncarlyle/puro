import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration
import kotlin.io.path.name
import kotlin.io.path.exists
import kotlin.math.min

// Will need version,
data class PuroRecord(
    val topic: String,
    val key: ByteBuffer,
    val value: ByteBuffer,
)

fun createRecordBuffer(record: PuroRecord): ByteBuffer {
    val (topic, key, value) = record
    rewindAll(key, value)

    // Should have the lengths, CRCs ready to go - should hold the lock for as short a time as possible
    val encodedTopic = topic.encodeToByteArray()
    val topicLength = encodedTopic.size
    val encodedTopicLength = topicLength.toVlqEncoding()
    val keyLength = key.capacity()
    val encodedKeyLength = keyLength.toVlqEncoding()
    val valueLength = value.capacity()

    val totalLength =
        encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
    val encodedTotalLength = totalLength.toVlqEncoding()

    // crc8 + totalLength + (topicLength + topic + keyLength + key + value)
    val recordBuffer = ByteBuffer.allocate(1 + encodedTotalLength.capacity() + totalLength)

    val messageCrc = getMessageCrc(
        encodedTotalLength = encodedTotalLength,
        encodedTopicLength = encodedTopicLength,
        encodedTopic = encodedTopic,
        encodedKeyLength = encodedKeyLength,
        key = key,
        value = value
    )
    rewindAll(encodedTotalLength, encodedTopicLength, encodedKeyLength, key, value)

    recordBuffer.put(messageCrc).put(encodedTotalLength).put(encodedTopicLength).put(encodedTopic)
        .put(encodedKeyLength).put(key).put(value)

    return recordBuffer.rewind()
//    var m: ArrayList<Byte> = ArrayList()
//    m.add(messageCrc)
//    encodedTotalLength.array().forEach { m.add(it) }
//    encodedTopicLength.array().forEach { m.add(it) }
//    encodedTopic.forEach { m.add(it) }
//    encodedKeyLength.array().forEach { m.add(it) }
//    key.array().forEach { m.add(it) }
//    value.array().forEach { m.add(it) }
//     recordBuffer.rewind()
//    val recordArray = ByteArray(recordBuffer.remaining())
//    recordBuffer.get(recordArray)
//    recordBuffer.rewind()
//    val mArray = m.toByteArray()
//    println("Record: ${recordArray.contentToString()}")
//    println("M:      ${mArray.contentToString()}")
//    println("Equal:  ${recordArray.contentEquals(mArray)}")
}

fun getMessageCrc(
    encodedTotalLength: ByteBuffer,
    encodedTopicLength: ByteBuffer,
    encodedTopic: ByteArray,
    encodedKeyLength: ByteBuffer,
    key: ByteBuffer,
    value: ByteBuffer,
): Byte =
    crc8(encodedTotalLength) //TODO get on same page about using buffers here - concerned of `ByteBuffer#array` cost
        .withCrc8(encodedTopicLength)
        .withCrc8(encodedTopic)
        .withCrc8(encodedKeyLength)
        .withCrc8(key)
        .withCrc8(value)

//  is the number of messages to be sent before reliquishing the lock
// This should have a maximum value to prevent starvation
// Probably better to return null/failing result for misconfiguration
class PuroProducer(
    val streamDirectory: String,
    val maximumWriteBatchSize: Int,
) {
    // Currently assuming is on active segment
    private val fileSystem: FileSystem = FileSystems.getDefault()

    // Subject to change with rotation
    companion object {
        // This should be configurable
        val retryDelay = 10.milliseconds.toJavaDuration()
    }

    fun extractSegmentName(path: Path) = path.fileName.toString().substringAfter("stream").substringBefore(".puro").toIntOrNull() ?: -1
    val compareBySegmentName =  Comparator<Path> { p1, p2 -> extractSegmentName(p1) - extractSegmentName(p2) }

    fun getActiveSegment(): Path {
        val streamDir = fileSystem.getPath(streamDirectory)
        Files.createDirectories(streamDir)

        val topFoundSegment = Files.list(streamDir).use { stream ->
            stream.filter { it.fileName.toString().matches(Regex("""stream\d+\.puro""")) }.max(compareBySegmentName)
        }

        return if (topFoundSegment.isPresent) {
            topFoundSegment.get()
        } else {
            streamDir.resolve("stream0.puro").also { Files.createFile(it) }
        }
    }

    // TODO actually test this
    fun send(puroRecord: PuroRecord) {
        val recordBuffer = createRecordBuffer(puroRecord)
        withLock { channel -> channel.write(recordBuffer) }
    }

    // TODO actually test this
    fun send(puroRecords: List<PuroRecord>) {
        val stepCount = if (puroRecords.size % maximumWriteBatchSize == 0) {
            puroRecords.size / maximumWriteBatchSize // 49/7 = 49
        } else {
            (puroRecords.size / maximumWriteBatchSize) + 1 // maximum modulo is one smaller than the batch
        }

        for (step in 0..stepCount) {
            val indices = step * maximumWriteBatchSize..<min((step + 1) * maximumWriteBatchSize, puroRecords.size)
            val records = puroRecords.slice(indices).map { createRecordBuffer(it) }
            withLock { channel -> records.forEach { channel.write(it) } }
        }
    }

    fun withLock(block: (FileChannel) -> Unit) {
        // There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
        // the active segment could change.
        // This has been marked on the README as 'Active segment transition race condition handling'
        // Best way may be to read if 'tombstone'
        FileChannel.open(getActiveSegment(), StandardOpenOption.APPEND).use { channel ->
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
