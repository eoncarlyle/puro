package com.iainschmitt

import crc8
import toVlqEncoding
import updateCrc8
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

// Will need version,
class Record<K, V>(
    val topic: String,
    val key: K,
    val value: V
)

// TODO
fun ByteBuffer.crc8(): ByteBuffer {
    return ByteBuffer.allocate(0)
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
        .updateCrc8(encodedTopicLength)
        .updateCrc8(encodedTopic)
        .updateCrc8(encodedKeyLength)
        .updateCrc8(key)
        .updateCrc8(value)

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

    fun send(topic: String, key: ByteBuffer, value: ByteBuffer): Unit {
        // Assuming on active segment at this point

        // Should have the lengths, CRCs ready to go - should hold the lock for as short a time as possible

        val encodedTopic = topic.encodeToByteArray()
        val topicLength = encodedTopic.size
        val encodedTopicLength = topicLength.toVlqEncoding()
        // This maybe should change - this will only work if the byte buffers are at capacity
        val keyLength = key.capacity()
        val encodedKeyLength = keyLength.toVlqEncoding()
        val valueLength = value.capacity()

        val totalLength =
            encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
        val encodedTotalLength = totalLength.toVlqEncoding()

        // crc8 + totalLength + (topicLength + topic + keyLength + key + value)
        val messageBuffer = ByteBuffer.allocate(1 + encodedTotalLength.capacity() + totalLength)

        //* TODO pick up here
        // I want to compute the CRC for everything after the CRC
        // But I don't want to call `crc8` because that requires a whole byte buffer
        // I'm 90% sure this isn't an issue at all because the crcs can be incrementally update
        // as is shown in updateCrc8 - will revisit tomorrow
        val messageCrc = getMessageCrc(
            encodedTotalLength = encodedTotalLength,
            encodedTopicLength = encodedTopicLength,
            encodedTopic = encodedTopic,
            encodedKeyLength = encodedKeyLength,
            key = key,
            value = value
        )

        FileChannel.open(streamFilePath, StandardOpenOption.APPEND).use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
        }
    }
}
