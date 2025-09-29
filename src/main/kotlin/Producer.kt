package com.iainschmitt

import crc8
import toVlqEncoding
import withCrc8
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

// Will need version,
data class PuroRecord(
    val topic: String,
    val key: ByteBuffer,
    val value: ByteBuffer,
)

fun createRecordBuffer(record: PuroRecord): ByteBuffer {
    val (topic, key, value) = record
    key.rewind()
    value.rewind()

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

    recordBuffer.put(messageCrc).put(encodedTotalLength.rewind()).put(encodedTopicLength.rewind()).put(encodedTopic)
        .put(encodedKeyLength.rewind()).put(key.rewind()).put(value.rewind())


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

    return recordBuffer.rewind()
}

fun getMessageCrc(
    encodedTotalLength: ByteBuffer,
    encodedTopicLength: ByteBuffer,
    encodedTopic: ByteArray,
    encodedKeyLength: ByteBuffer,
    key: ByteBuffer,
    value: ByteBuffer,
): Byte = crc8(encodedTotalLength) //TODO get on same page about using buffers here - concerned of `ByteBuffer#array` cost
        .withCrc8(encodedTopicLength)
        .withCrc8(encodedTopic)
        .withCrc8(encodedKeyLength)
        .withCrc8(key)
        .withCrc8(value)

class PuroProducer(
    streamFileName: String,
) {
    // Currently assuming is on active segment
    private val fileSystem: FileSystem = FileSystems.getDefault()
    private val streamFilePath = fileSystem.getPath(streamFileName)

    companion object {
        // This should be configurable
        val retryDelay = 10.milliseconds.toJavaDuration()
    }

    fun send(puroRecord: PuroRecord) {
        // Assuming on active segment at this point

        val recordBuffer = createRecordBuffer(puroRecord)
        FileChannel.open(streamFilePath, StandardOpenOption.APPEND).use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            channel.write(recordBuffer)
        }
    }
}
