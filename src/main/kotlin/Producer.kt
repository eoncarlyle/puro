import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
    private val logger: Logger = NOPLogger.NOP_LOGGER
    var times = 0;

    companion object {
        // This should be configurable?
        private val retryDelay = 10.milliseconds.toJavaDuration()
    }

    constructor(
        streamDirectory: Path,
        maximumWriteBatchSize: Int,
        readBufferSize: Int = 8192,
        logger: Logger = NOPLogger.NOP_LOGGER
    ) {
        this.offset = 0
        this.streamDirectory = streamDirectory
        this.maximumWriteBatchSize = maximumWriteBatchSize
        this.readBufferSize = readBufferSize
        this.readBuffer = ByteBuffer.allocate(readBufferSize)
        this.currentSegmentOrder = getHighestSegmentOrder(streamDirectory, readBuffer, retryDelay, logger)
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
            val batchedBuffer = createBatchedSignalRecordBuffer(puroRecords.slice(indices), logger)
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
                withFileLockDimensions(channel, retryDelay, logger) { lockStart, fileSizeOnceLockAcquired ->
                    val channelPosition = if (lockStart > 0) {
                        lastBlockIntegrityCheckOrCleanup(channel, lockStart, fileSizeOnceLockAcquired) ?: fileSizeOnceLockAcquired
                    } else {
                        fileSizeOnceLockAcquired
                    }
                    channel.position(channelPosition)
                    block(channel)
                }
            }
    }

    // Returns a long if offset needs to be adjusted
    private fun lastBlockIntegrityCheckOrCleanup(
        channel: FileChannel,
        lockStart: Long,
        fileSizeOnceLockAcquired: Long
    ): Long? {
        readBuffer.clear()
        channel.read(readBuffer, lockStart)
        val maybeBlockEndRecord = getRecords(
            readBuffer,
            0, //lockStart,
            fileSizeOnceLockAcquired - lockStart,
            listOf(ControlTopic.BLOCK_END.value),
            true
        )
        readBuffer.flip()

        val blockEndRecord = when (maybeBlockEndRecord) {
            is GetRecordsResult.Success -> {
                if (maybeBlockEndRecord.records.size == 1) {
                    maybeBlockEndRecord.records.first
                } else throw NotImplementedError("Segment cleanup not implemented")
            } //TODO segment tombstoning
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

        val maybeBlockStartRecord = getRecords(
            readBuffer,
            0,
            BLOCK_START_RECORD_SIZE.toLong(),
            listOf(ControlTopic.BLOCK_START.value),
            true
        )
        readBuffer.flip()

        when (maybeBlockStartRecord) {
            // Load bearing to confirm that a standard abonormality will be returned TODO tests to establish this
            is GetRecordsResult.Success -> logger.info("Confirmed segment integrity")
            is GetRecordsResult.StandardAbnormality -> {
                if (maybeBlockStartRecord.abnormality == GetRecordsAbnormality.LowSignalBit) {
                    val firstBadByte = lockStart - subBlockSize
                    logger.warn("Truncating segment to $firstBadByte bytes")
                    channel.truncate(firstBadByte)
                    return firstBadByte
                } else {
                    throw IllegalStateException("Unexpected or corrupted segment state")
                }
            }
            else -> throw IllegalStateException("Unexpected or corrupted segment state")
        }

        return null
    }
}
