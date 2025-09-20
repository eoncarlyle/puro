package com.iainschmitt

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

class Record<K,V>(
    val key: K,
    val value: V
)

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

    fun <K,V> send(key: ByteBuffer, value: ByteBuffer): Unit {

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