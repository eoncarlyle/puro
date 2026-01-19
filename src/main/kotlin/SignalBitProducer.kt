import org.slf4j.Logger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.experimental.and
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

data class SerialisedPuroRecord(
    val messageCrc: Byte,
    val encodedSubrecordLength: ByteBuffer,
    val encodedTopicLength: ByteBuffer,
    val encodedTopic: ByteArray,
    val encodedKeyLength: ByteBuffer,
    val key: ByteBuffer,
    val value: ByteBuffer
)

class SignalBitProducer {
    val streamDirectory: Path
    val maximumWriteBatchSize: Int
    val readBufferSize: Int
    private var currentSegmentOrder: Int
    private var offset: Int
    private val readBuffer: ByteBuffer

    companion object {
        // This should be configurable?
        private val retryDelay = 10.milliseconds.toJavaDuration()
    }

    constructor(
        streamDirectory: Path,
        maximumWriteBatchSize: Int,
        readBufferSize: Int = 8192
    ) {
        this.offset = 0
        this.streamDirectory = streamDirectory
        this.maximumWriteBatchSize = maximumWriteBatchSize
        this.readBufferSize = readBufferSize
        this.currentSegmentOrder = getHighestSegmentOrder(streamDirectory)
        this.readBuffer = ByteBuffer.allocate(readBufferSize)
    }

    private fun getStepCount(puroRecords: List<PuroRecord>) = if (puroRecords.size % maximumWriteBatchSize == 0) {
        puroRecords.size / maximumWriteBatchSize // 49/7 = 7
    } else {
        (puroRecords.size / maximumWriteBatchSize) + 1 // maximum modulo is one smaller than the batch
    }

    fun send(puroRecords: List<PuroRecord>) {
        val stepCount = this.getStepCount(puroRecords)
        for (step in 0..<stepCount) {
            val indices =
                step * this.maximumWriteBatchSize..<min((step + 1) * this.maximumWriteBatchSize, puroRecords.size)
            val batchedBuffer = createBatchedSignalRecordBuffer(puroRecords.slice(indices))
            this.withProducerLock { channel ->
                val initialPosition = channel.position()
                channel.write(batchedBuffer)

                val signalRecord = PuroRecord(
                    ControlTopic.BLOCK_START.value, ByteBuffer.wrap(byteArrayOf()), ByteBuffer.wrap(
                        byteArrayOf(0x01)
                    )
                ).toSerialised()

                channel.write(ByteBuffer.wrap(byteArrayOf(signalRecord.first.messageCrc)), initialPosition)
                channel.write(ByteBuffer.wrap(byteArrayOf(0x01)), initialPosition + BLOCK_START_RECORD_SIZE - 1)
            }
        }
    }

    private fun withProducerLock(block: (FileChannel) -> Unit) {

        FileChannel.open(getActiveSegment(streamDirectory, true), StandardOpenOption.WRITE, StandardOpenOption.READ)
            .use { channel ->
                val initialFileSize = channel.size()

                var lock: FileLock?

                val blockEndOffset = (initialFileSize - BLOCK_END_RECORD_SIZE + 1).coerceAtLeast(0)

                do {
                    lock = channel.tryLock(
                        blockEndOffset,
                        Long.MAX_VALUE - blockEndOffset,
                        false
                    )
                    if (lock == null) {
                        Thread.sleep(retryDelay) // Should eventually give up
                    }
                } while (lock == null)

                val fileSizeOnceLockAcquired = channel.size()

                val lockStart = if (fileSizeOnceLockAcquired >= BLOCK_END_RECORD_SIZE) {
                    fileSizeOnceLockAcquired - BLOCK_END_RECORD_SIZE + 1
                } else {
                    0L
                }

                if (lockStart > 0) {
                    confirmLastBlockIntegrity(channel, lockStart, fileSizeOnceLockAcquired)
                }

                block(channel)
            }
    }

    private fun confirmLastBlockIntegrity(
        channel: FileChannel,
        lockStart: Long,
        fileSizeOnceLockAcquired: Long
    ) {
        readBuffer.clear()
        channel.read(readBuffer, lockStart)
        val maybeBlockEndRecord = getSignalBitRecords(
            readBuffer,
            0, //lockStart,
            fileSizeOnceLockAcquired - lockStart,
            listOf(ControlTopic.BLOCK_END.value),
            true,
            null
        )
        readBuffer.flip()

        val blockEndRecord = when (maybeBlockEndRecord) {
            is GetSignalRecordsResult.Success -> {
                if (maybeBlockEndRecord.records.size == 1) {
                    maybeBlockEndRecord.records.first
                } else throw NotImplementedError("Segment cleanup not implemented")
            }

            else -> throw NotImplementedError("Segment cleanup not implemented")
        }

        val subBlockSize = blockEndRecord.value.getInt()

        var readLock: FileLock?
        do {
            readLock = channel.tryLock(lockStart - subBlockSize, BLOCK_START_RECORD_SIZE.toLong(), true)
        } while (readLock == null)

        readBuffer.clear()
        readBuffer.limit(BLOCK_START_RECORD_SIZE)
        channel.read(readBuffer, lockStart - subBlockSize)

        val maybeBlockStartRecord = getSignalBitRecords(
            readBuffer,
            0,
            BLOCK_START_RECORD_SIZE.toLong(),
            listOf(ControlTopic.BLOCK_START.value),
            true,
            null
        )
        readBuffer.flip()

        when (maybeBlockStartRecord) {
            is GetSignalRecordsResult.Success -> {
                if (maybeBlockStartRecord.records.size == 1) {
                    maybeBlockStartRecord.records.first.value.rewind()
                    assert(
                        (maybeBlockStartRecord.records.first.value.get() and 0x01) == 1.toByte()
                    ) { "Low signal bit" }
                } else throw NotImplementedError("Segment cleanup not implemented")
            }

            else -> throw NotImplementedError("Segment cleanup not implemented")
        }
    }
}
