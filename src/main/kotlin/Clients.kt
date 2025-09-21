package com.iainschmitt

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

fun ceilingDivision(dividend: Int, divisor: Int): Int = (dividend + divisor - 1) / divisor
fun Long.get7Bit(index: Int): Byte = (this shr (index * 7) and 0x7F).toByte()
fun Long.last7Bit(): Byte = (this and 0x7F).toByte()

// https://en.wikipedia.org/wiki/Variable-length_quantity
fun Long.toVlq(): ByteBuffer {
    // !! Need to throw if the long conversion doesn't work: may not even be possible with longs
    val bitsReq = Long.SIZE_BITS - this.countLeadingZeroBits()
    val bytesReq = ceilingDivision(bitsReq, 7)
    if (bytesReq == 1) {
        ByteBuffer.allocate(1).put(this.last7Bit())
    }
    val finalBuffer = ByteBuffer.allocate(bytesReq)
    //'Little-endian byte order allows us to support arbitrary lengths more easily,' from Kafka source
    for (i in 0..<bytesReq - 1) {
        // 0x80: continuation bit
        finalBuffer.put((0x80 + this.get7Bit(i)).toByte())
    }
    finalBuffer.put(this.get7Bit(bytesReq - 1))
    return finalBuffer
}

class PuroProducer(
    streamFileName: String,
) {
    // Currently assuming is on active segment
    private val fileSystem: FileSystem = FileSystems.getDefault()
    private val streamFilePath = fileSystem.getPath(streamFileName)

    companion object {
        val retryDelay = 10.milliseconds.toJavaDuration()
    }

    fun <K, V> send(topic: String, key: ByteBuffer, value: ByteBuffer): Unit {
        // Assuming is on active segment
        FileChannel.open(streamFilePath, StandardOpenOption.APPEND).use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay)
                }
            } while (lock == null)

            // Need to understand byte buffers better for rest of this -
            // https://kafka.apache.org/documentation/#record
            // key.capacity() / value.capaciy()
        }
    }
}