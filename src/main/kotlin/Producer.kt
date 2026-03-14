import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

class Producer {
    val streamDirectory: Path
    val maximumWriteBatchSize: Int
    val readBufferSize: Int
    private var currentSegmentOrder: Int
    private var offset: Int
    private val readBuffer: ByteBuffer
    private var logger: Logger = NOPLogger.NOP_LOGGER
    var times = 0;
    var producerState: ProducerSegmentState = ProducerSegmentState.Init

    private var startRecordArray = ByteArray(BLOCK_START_RECORD_SIZE - 1)

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
        this.logger = logger
        this.currentSegmentOrder = getHighestSegmentOrder(streamDirectory, readBuffer, retryDelay, logger)

        val initialIntegrity = getInitialSegmentIntegrity()
        this.producerState = when (initialIntegrity) {
            is Either.Left -> throw RuntimeException("!!") //ProducerSegmentState.Cleanup(initialIntegrity.value)
            is Either.Right -> ProducerSegmentState.Ready(initialIntegrity.value)
        }
    }

    // See Consumer#withConsumerLock
    private fun getInitialSegmentIntegrity(): Either<Long, Long> {
        if (currentSegmentOrder == -1) {
            return right(0)
        }

        val path = getSegmentPath(streamDirectory, currentSegmentOrder)
            ?: throw IllegalStateException("Segment $currentSegmentOrder deleted before use")
        var lock: FileLock? = null
        var knownSegmentSize: Long? = null
        //TODO segment rollover awareness
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            do {
                try {
                    knownSegmentSize = channel.size()
                    lock = channel.tryLock(offset.toLong(), knownSegmentSize!!, true) //Saftey: single thread modify
                } catch (_: OverlappingFileLockException) {
                    logger.warn("Hit OverlappingFileLockException, should only happen when testing mutliple clients in same JVM")
                    Thread.sleep(retryDelay)
                }
            } while (lock == null)
            return getSegmentIntegrity(channel, knownSegmentSize!!, offset.toLong()) //Saftey: see  above
        }
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
            val serialiseRecordBatch = getSerialiseRecordBatch(
                puroRecords.slice(indices),
                logger
            )
            this.withProducerLock { channel ->
                val initialPosition = channel.position()
                channel.write(serialiseRecordBatch)

                serialiseRecordBatch.rewind().get(1, startRecordArray)
                startRecordArray[BLOCK_START_RECORD_SIZE - 6] = 0x01
                val finalBlockStartRecordCrc8 = crc8(startRecordArray)

                val finalRecord = ByteBuffer.wrap(ByteArray(BLOCK_START_RECORD_SIZE))
                finalRecord.put(finalBlockStartRecordCrc8)
                finalRecord.put(startRecordArray)
                finalRecord.rewind()
                getRecord(finalRecord)

                channel.write(ByteBuffer.wrap(byteArrayOf(finalBlockStartRecordCrc8)), initialPosition)
                logger.info("Block Start CRC/Position: ${finalBlockStartRecordCrc8}/$initialPosition")
                channel.write(ByteBuffer.wrap(byteArrayOf(0x01)), initialPosition + BLOCK_START_RECORD_SIZE - 5)
            }
        }
    }

    private fun withProducerLock(block: (FileChannel) -> Unit) {
        FileChannel.open(getActiveSegment(streamDirectory, true), StandardOpenOption.WRITE, StandardOpenOption.READ)
            .use { channel ->
                withFileLockDimensions(channel, retryDelay, logger) { lockStart, fileSizeOnceLockAcquired ->
                    val channelPosition = if (lockStart > 0) {
                        lastBlockIntegrityCheckOrCleanup(channel, lockStart, fileSizeOnceLockAcquired)
                            ?: fileSizeOnceLockAcquired
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

    private fun buildProtoRecordAtIndex(
        index: Int,
        record: PuroRecord,
        protoRecords: Array<SerialisedPuroRecord?>
    ): Int {
        val (serialisedRecord, recordLength) = record.toSerialised()
        protoRecords[index] = serialisedRecord
        return recordLength
    }

    // TODO: See `createRecordBuffer` logic in test Utils
    private fun getSerialiseRecordBatch(
        puroRecords: List<PuroRecord>,
        logger: Logger?
    ): ByteBuffer {
        val protoRecords = Array<SerialisedPuroRecord?>(puroRecords.size + 2) { null }
        val blockBodySize =// need to shift over 1 for starting record
            puroRecords.mapIndexed { index, record -> buildProtoRecordAtIndex(index + 1, record, protoRecords) }
                .sum()

        val subBlockLength = BLOCK_START_RECORD_SIZE + blockBodySize
        val startBlockValue = ByteBuffer.allocate(5).put(0x00).putInt(subBlockLength).rewind()
        buildProtoRecordAtIndex(
            0,
            PuroRecord(ControlTopic.BLOCK_START.value, ByteBuffer.wrap(byteArrayOf()), startBlockValue),
            protoRecords
        )

        val endBlockValue = ByteBuffer.allocate(4)
        endBlockValue.putInt(subBlockLength)

        val endBlockRecordSize = buildProtoRecordAtIndex(
            puroRecords.size + 1,
            PuroRecord(ControlTopic.BLOCK_END.value, ByteBuffer.wrap(byteArrayOf()), endBlockValue),
            protoRecords
        )
        //TODO Remove me at some point this is just for logging
        val crcs = arrayListOf<Byte>()
        val batchBuffer = ByteBuffer.allocate(subBlockLength + endBlockRecordSize)

        protoRecords.forEach { record: SerialisedPuroRecord? ->
            val (messageCrc,
                encodedSubrecordLength,
                encodedTopicLength,
                encodedTopic,
                encodedKeyLength,
                key,
                value) = record!!
            crcs.add(messageCrc)
            batchBuffer.put(messageCrc).put(encodedSubrecordLength).put(encodedTopicLength).put(encodedTopic)
                .put(encodedKeyLength).put(key).put(value)
        }
        crcs.slice(1..<crcs.size).forEach { logger?.info("Record CRC: $it") }
        return batchBuffer.rewind()
    }

    private fun getSegmentIntegrity(
        channel: FileChannel,
        fileSizeOnceLockAcquired: Long,
        initialOffset: Long
    ): Either<Long, Long> {
        var currentOffset = initialOffset
        var currentBlockStart = currentOffset
        var blockStartSubblockSize = 0
        var blockEndSubblockSize = 0

        while (currentOffset < fileSizeOnceLockAcquired) {
            readBuffer.rewind()
            readBuffer.limit(BLOCK_START_RECORD_SIZE)
            currentBlockStart = currentOffset
            if ((currentOffset + BLOCK_START_RECORD_SIZE) > fileSizeOnceLockAcquired) {
                break
            }

            channel.read(readBuffer, currentOffset)
            val maybeBlockStartRecord = getRecords(
                readBuffer,
                0,
                BLOCK_START_RECORD_SIZE.toLong(),
                listOf(ControlTopic.BLOCK_START.value),
                true
            )

            when (maybeBlockStartRecord) {
                is GetRecordsResult.Success -> {
                    if (maybeBlockStartRecord.records.size == 1) {
                        val signalByte = maybeBlockStartRecord.records.first.value.get()
                        if (signalByte != 1.toByte()) {
                            break
                        }
                        blockStartSubblockSize = maybeBlockStartRecord.records.first.value.getInt()
                        currentOffset += blockStartSubblockSize
                    } else break
                }

                else -> break
            }

            readBuffer.rewind()
            readBuffer.limit(BLOCK_END_RECORD_SIZE)

            if ((currentOffset + BLOCK_END_RECORD_SIZE) > fileSizeOnceLockAcquired) {
                break
            }

            channel.read(readBuffer, currentOffset)
            val maybeBlockEndRecord = getRecords(
                readBuffer,
                0,
                BLOCK_END_RECORD_SIZE.toLong(),
                listOf(ControlTopic.BLOCK_END.value),
                true
            )

            when (maybeBlockEndRecord) {
                is GetRecordsResult.Success -> {
                    if (maybeBlockStartRecord.records.size == 1) {
                        blockEndSubblockSize = maybeBlockEndRecord.records.first.value.getInt()
                        if (blockEndSubblockSize != blockStartSubblockSize) {
                            break
                        }
                        currentBlockStart = currentOffset
                        currentOffset += blockEndSubblockSize
                    } else break
                }

                else -> break
            }

            if (currentOffset == fileSizeOnceLockAcquired - BLOCK_END_RECORD_SIZE) {
                return right(fileSizeOnceLockAcquired)
            } else if (currentOffset > fileSizeOnceLockAcquired - BLOCK_END_RECORD_SIZE) {
                break
            }
        }
        return if (currentOffset != initialOffset) {
            left(currentBlockStart)
        } else right(currentOffset)
    }
}