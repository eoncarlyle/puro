package com.iainschmitt

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

class Record<K, V>(
    val key: K,
    val value: V
)

fun ceilingDivision(dividend: Long, divisor: Long): Int = ((dividend + divisor - 1) / divisor).toInt()
fun Long.toBuffer(bytesReq: Int): ByteBuffer = ByteBuffer.allocate(bytesReq).putLong(this)
fun Long.get7Bit(index: Int): Byte = (this shr (index * 7) and 0x7F).toByte()
fun Long.last7Bit(): Byte = (this and 0x7F).toByte()

// https://en.wikipedia.org/wiki/Variable-length_quantity
// Little-endian I guess
fun Long.toVlq(): ByteBuffer {
    val bitsReq = (Long.SIZE_BITS - this.countLeadingZeroBits()).toLong()
    // Need to throw if the long conversion doesn't work: may not even be possible with longs
    val bytesReq = ceilingDivision(bitsReq, 7)
    if (bytesReq == 1) {
        ByteBuffer.allocate(1).put(this.last7Bit())
    }
    // Lots of int -> long, vise versa
    val finalBuffer = ByteBuffer.allocate(bytesReq)
    //'Little-endian byte order allows us to support arbitrary lengths more easily,'
    // https://github.com/apache/kafka/blob/5919762009a0bbc4948bf881a6fbd885e30527b1/trogdor/src/main/java/org/apache/kafka/trogdor/workload/SequentialPayloadGenerator.java#L44
    for (i in 0..<bytesReq - 1) {
        finalBuffer.put(this.get7Bit(i))
    }
    finalBuffer.put(this.get7Bit(bytesReq - 1))
    return finalBuffer
}

// RandomAccessFile, FileChannel `tryLock`
class PuroProducer(
    streamFileName: String,
) {
    // Assuming is on active segment
    private val fileSystem: FileSystem = FileSystems.getDefault()
    private val streamFilePath = fileSystem.getPath(streamFileName)

    companion object {
        val retryDelay = 10.milliseconds.toJavaDuration()
    }

    fun <K, V> send(key: ByteBuffer, value: ByteBuffer): Unit {
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