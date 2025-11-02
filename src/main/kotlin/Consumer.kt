import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.Logger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.PriorityBlockingQueue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

sealed class ConsumerStart {
    data object StreamBeginning : ConsumerStart()
    data object Latest : ConsumerStart()
}

sealed class ConsumerError {
    data object FailingCrc : ConsumerError()
    data object NoRemainingBuffer : ConsumerError()
    data object HardTransitionFailure : ConsumerError()
}

sealed class GetRecordAbnormality {
    data object Truncation : GetRecordAbnormality()
    data object RecordsAfterTombstone : GetRecordAbnormality()
    data object StandardTombstone : GetRecordAbnormality()
}

private sealed class ReadTombstoneStatus {
    data object NotTombstoned : ReadTombstoneStatus()
    data object RecordsAfterTombstone : ReadTombstoneStatus()
    data object StandardTombstone : ReadTombstoneStatus()
}

private data class StandardRead(
    val finalConsumerOffset: Long,
    val fetchedRecords: ArrayList<PuroRecord>,
    val abnormalOffsetWindow: Pair<Long, Long>?,
    val recordsAfterTombstone: ReadTombstoneStatus,
)

private sealed class FetchSuccess() {
    data object CleanFetch : FetchSuccess()
    data object CleanSegmentClosure : FetchSuccess()
    data object RecordsAfterTombstone : FetchSuccess()
    class HardTransition(
        val abnormalOffsetWindowStart: Long,
        val abnormalOffsetWindowStop: Long,
        val recordsAfterTombstone: Boolean
    ) : FetchSuccess()
}

typealias ConsumerResult<R> = Either<ConsumerError, R>

fun getRecord(recordBuffer: ByteBuffer): ConsumerResult<Pair<PuroRecord, Int>> {
    if (!recordBuffer.hasRemaining()) {
        return left(ConsumerError.NoRemainingBuffer)
    }
    val start = recordBuffer.position()
    val expectedCrc = recordBuffer.get()
    val (encodedTotalLength, _, crc1) = recordBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = recordBuffer.fromVlq()
    val (topic, crc3) = recordBuffer.getArraySlice(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = recordBuffer.fromVlq()
    val (key, crc5) = recordBuffer.getBufferSlice(keyLength)
    val (value, crc6) = recordBuffer.getBufferSlice(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        right(PuroRecord(topic, key, value) to (recordBuffer.position() - start))
    } else left(ConsumerError.FailingCrc)
}

fun getTopicOnPossiblyTruncatedMessage(recordBuffer: ByteBuffer): ByteArray {
    // TODO saftey
    recordBuffer.position(1)
    recordBuffer.fromVlq() //Discarding the length but advancing buffer
    val (topicLength) = recordBuffer.fromVlq()
    val (topic) = recordBuffer.getArraySlice(topicLength)
    return topic
}

fun isRelevantTopic(
    topic: ByteArray,
    subscribedTopics: List<ByteArray>,
): Boolean = subscribedTopics.any { it.contentEquals(topic) } || ControlTopic.entries.toTypedArray()
    .any { it.value.contentEquals(topic) }

fun getRecords(
    readBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long,
    subscribedTopics: List<ByteArray>,
    logger: Logger,
    isEndOfFetch: Boolean = false
): Triple<List<PuroRecord>, Long, GetRecordAbnormality?> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var truncationAbnormality = false // Only matters if end-of-fetch

    readBuffer.position(initialOffset.toInt()) //Saftey issue
    readBuffer.limit(finalOffset.toInt()) //Saftey issue

    while (readBuffer.hasRemaining()) {
        val expectedCrc = readBuffer.get()

        //val (encodedTotalLength, encodedTotalLengthBitCount, crc1) = byteBuffer.fromVlq()
        val lengthData = readBuffer.readSafety()?.fromVlq()
        //val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
        val topicLengthData = readBuffer.readSafety()?.fromVlq()
        //val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
        val topicMetadata = if (topicLengthData != null) {
            readBuffer.readSafety()?.getSafeArraySlice(topicLengthData.first)
        } else null

        //TODO: Subject this to microbenchmarks, not sure if this actually matters
        if (topicMetadata == null || !isRelevantTopic(topicMetadata.first, subscribedTopics)) {
            if (lengthData != null && (1 + lengthData.second + lengthData.first) <= readBuffer.remaining()) {
                offset += (1 + lengthData.second + lengthData.first)
                continue
            }
        }
        //val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
        val keyMetdata = readBuffer.readSafety()?.fromVlq()
        // val (key, crc5) = byteBuffer.getSubsequence(keyLength)
        val keyData = if (keyMetdata != null) {
            readBuffer.readSafety()?.getBufferSlice(keyMetdata.first)
        } else null
        // val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)
        val valueData = if (lengthData != null && topicLengthData != null && keyMetdata != null) {
            readBuffer.readSafety()
                ?.getBufferSlice(lengthData.first - topicLengthData.second - topicLengthData.first - keyMetdata.second - keyMetdata.first)
        } else null

        // The else branch isn't advancing the offset because it is possible that this is the next batch
        // TODO: while the reasoning above is sound, but we now have a baked in assumption that the only time
        // TODO: ...we will have bad messages is for the outside of fetches. Should throw if this is not the case,
        // TODO: ...this is now maked as 'Fetch interior failures' in the readme
        if (lengthData != null && topicLengthData != null && topicMetadata != null && keyMetdata != null && keyData != null && valueData != null) {
            val (subrecordLength, encodedTotalLengthBitCount, crc1) = lengthData
            val (topicLength, topicLengthBitCount, crc2) = topicLengthData
            val (topic, crc3) = topicMetadata
            val (keyLength, keyLengthBitCount, crc4) = keyMetdata
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)
            // Equivalent to:
            //val subrecordLength = topicLengthBitCount + topicLength + keyLengthBitCount + keyLength + value.capacity()
            val totalLength = 1 + encodedTotalLengthBitCount + subrecordLength

            if ((expectedCrc == actualCrc) && subscribedTopics.any { it.contentEquals(topic) }) {
                records.add(PuroRecord(topic, key, value))
            } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic)) {
                val abnormality = if (isEndOfFetch || readBuffer.hasRemaining()) {
                    GetRecordAbnormality.RecordsAfterTombstone
                } else {
                    GetRecordAbnormality.StandardTombstone
                }
                return Triple(records, offset, abnormality)
            }

            // These are necessarily 'interior' messages.
            offset += totalLength
        } else {
            truncationAbnormality = true
        }
    }

    return Triple(records, offset, if (isEndOfFetch && truncationAbnormality) GetRecordAbnormality.Truncation else null)
}

fun hardTransitionSubrecordLength(subrecordLengthMetaLengthSum: Int): Int {
    //messageSize = 1 + capacity(vlq(subrecordLength)) + subrecordLength
    var subrecordLength = subrecordLengthMetaLengthSum - 1

    while (subrecordLengthMetaLengthSum != 1 + ceilingDivision(
            Int.SIZE_BITS - subrecordLength.countLeadingZeroBits(),
            7
        ) + subrecordLength
    ) {
        subrecordLength--
    }
    return subrecordLength
}

class PuroConsumer(
    val streamDirectory: Path,
    serialisedTopicNames: List<String>,
    val logger: Logger,
    val readBufferSize: Int = 8192,
    val startPoint: ConsumerStart = ConsumerStart.Latest,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) : Runnable {
    companion object {
        // This should be configurable?
        private val retryDelay = 10.milliseconds.toJavaDuration()

        // I don't have very good intuition on this one, should be benchmarked
        private val queueCapacity = 100_000
    }

    //TODO make this configurable:
    //var _consumedSegment = getActiveSegment(streamDirectory)
    var consumedSegmentOrder = when (startPoint) {
        is ConsumerStart.StreamBeginning -> getLowestSegmentOrder(streamDirectory)
        is ConsumerStart.Latest -> getLowestSegmentOrder(streamDirectory)
    }

    //First byte offset that hasn't been read
    var consumerOffset = if (startPoint == ConsumerStart.Latest && consumedSegmentOrder != -1) {
        Files.size(getConsumedSegmentPath())
    } else {
        0L
    }
    var currentConsumerLocked = consumedSegmentOrder == -1

    val readBuffer = ByteBuffer.allocate(readBufferSize)
    var abnormalOffsetWindow: Pair<Long, Long>? = null

    // PriorityBlockingQueue javadoc:
    // "Operations on this class make no guarantees about the ordering of elements with equal priority.
    // If you need to enforce an ordering, you can define custom classes or comparators that use a secondary key to
    // break ties in primary priority values"
    val compareConsumerSegmentPair =
        Comparator<ConsumerSegmentEvent> { p1, p2 ->
            return@Comparator (if (p1.segmentOrder != p2.segmentOrder) {
                p1.segmentOrder - p2.segmentOrder
            } else {
                (p1.offset - p2.offset).toInt()
            })
        }

    var segmentChangeQueue = PriorityBlockingQueue(queueCapacity, compareConsumerSegmentPair)
    var subscribedTopics = serialisedTopicNames.map { it.toByteArray() }

    //fun stopListening() = watcher?.close()

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            // Diagnostic logs
            // logger.info(event.path().toString())
            // logger.info(event.eventType().toString())
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    //TODO cache this, no reason to recompute
                    //val currentSegmentOrder = getSegmentOrder(consumedSegment)
                    val incomingSegmentOrder = getSegmentOrder(event.path())
                    val currentSegmentSize = Files.size(getConsumedSegmentPath())

                    if (event.path() == getSegmentPath(streamDirectory, consumedSegmentOrder)) {
                        segmentChangeQueue.offer(ConsumerSegmentEvent(currentSegmentSize, consumedSegmentOrder))
                    } else if (incomingSegmentOrder > consumedSegmentOrder) {
                        segmentChangeQueue.offer(
                            ConsumerSegmentEvent(
                                currentSegmentSize,
                                incomingSegmentOrder
                            )
                        )
                    } else if (incomingSegmentOrder == -1) {
                        logger.warn("Modify event for non-segment path ${event.path()} recorded")
                    } else {
                        logger.warn("Modify event for segment ${event.path()} recorded, possible damaged data integrity")
                    }
                }

                DirectoryChangeEvent.EventType.CREATE -> {} //May have active segment management concerns
                DirectoryChangeEvent.EventType.DELETE -> {} //Log something probably
                DirectoryChangeEvent.EventType.OVERFLOW -> {} //Log something probably
                else -> {} // This should never happen
            }
        }
        .build()

    override fun run() {
        logger.info("Starting Consumer")
        Thread { watcher?.watch() }.start()
        logger.info("Consumer Directory Watcher Configured")
        Thread {
            while (true) {
                val (producerOffset, incomingSegmentOrder) = segmentChangeQueue.take()

                if (currentConsumerLocked and (incomingSegmentOrder == consumedSegmentOrder)) {
                    logger.error("Ignored ConsumerSegmentEvent: segment locked, current order $consumedSegmentOrder")
                } else if (incomingSegmentOrder == consumedSegmentOrder) {
                    onConsumedSegmentAppend(producerOffset)
                } else if (currentConsumerLocked and (incomingSegmentOrder == consumedSegmentOrder + 1)) {
                    // The priority queue will ensure all records of order _N_ will be received before all records of
                    // order _N+1_
                    val nextSegment = getSegmentPath(streamDirectory, consumedSegmentOrder + 1)
                    if (nextSegment != null) {
                        consumedSegmentOrder++
                        currentConsumerLocked = false
                        onConsumedSegmentAppend(consumerOffset)
                    } else {
                        logger.error("Illegal ConsumerSegmentEvent: no segment present at order $incomingSegmentOrder")
                    }
                } else {
                    logger.error("Illegal ConsumerSegmentEvent: segment order $incomingSegmentOrder, at lock state $currentConsumerLocked")
                }
            }
        }.start()
    }

    private fun ArrayList<PuroRecord>.onMessages() {
        //TODO: remove the filter once the getMessage optimisation taken care of?
        this.filter { r -> subscribedTopics.any { it.contentEquals(r.topic) } }
            .forEach { r -> onMessage(r) }
    }

    fun onConsumedSegmentAppend(producerOffset: Long) {
        val records = ArrayList<PuroRecord>()

        // TODO: if the lambda is broken out it will be much easier to test
        val fetchResult = withConsumerLock(consumerOffset, producerOffset - consumerOffset) { fileChannel ->
            fetch(fileChannel, records, producerOffset)
        }

        fetchResult.onRight {
            when (it) {
                is FetchSuccess.CleanFetch -> {
                    records.onMessages()
                }

                is FetchSuccess.CleanSegmentClosure -> {
                    currentConsumerLocked = true
                    records.onMessages()
                }

                is FetchSuccess.RecordsAfterTombstone -> {
                    reterminateSegment()
                    currentConsumerLocked = true
                    records.onMessages()
                }

                is FetchSuccess.HardTransition -> {
                    onHardTransitionCleanup(it)
                    records.onMessages()
                }
            }
        }.onLeft {
            //TODO an actually acceptable logging message
            throw RuntimeException(it.toString())
        }
    }


    private fun happyPathFetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<FetchSuccess> {
        val (finalConsumerOffset, fetchedRecords, readAbnormalOffsetWindow, readTombstoneStatus) = standardRead(
            fileChannel,
            consumerOffset,
            producerOffset,
        )
        records.addAll(fetchedRecords)
        consumerOffset = finalConsumerOffset
        abnormalOffsetWindow = readAbnormalOffsetWindow
        currentConsumerLocked = readTombstoneStatus != ReadTombstoneStatus.NotTombstoned

        return if (readTombstoneStatus != ReadTombstoneStatus.RecordsAfterTombstone) {
            right(FetchSuccess.CleanFetch)
        } else {
            right(FetchSuccess.RecordsAfterTombstone)
        }
    }

    private fun fetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<FetchSuccess> {
        if (abnormalOffsetWindow == null) {
            return happyPathFetch(fileChannel, records, producerOffset)
        } else {
            readBuffer.clear()
            fileChannel.read(readBuffer)
            readBuffer.flip()
            // Continuation: clean message divide over a fetch boundary
            val continuationResult = getRecord(readBuffer)

            return continuationResult.fold(
                ifRight = { result ->
                    consumerOffset += result.second
                    abnormalOffsetWindow = null
                    return happyPathFetch(fileChannel, records, producerOffset)
                },
                ifLeft = {
                    // Hard producer transition: last producer failed, but new messages not interrupted
                    readBuffer.rewind()
                    // Off-by-one possibility
                    // TODO buffer safety: integer conversions may be an issue
                    val bufferOffset = (abnormalOffsetWindow!!.second - abnormalOffsetWindow!!.first).toInt()
                    readBuffer.position(bufferOffset + 1)

                    getRecord(readBuffer)
                        .fold(
                            ifLeft = {
                                Either.Left<ConsumerError, FetchSuccess>(ConsumerError.HardTransitionFailure)
                            },
                            ifRight = { hardProducerTransitionResult ->
                                consumerOffset += hardProducerTransitionResult.second

                                val originalAbnormalOffsetWindow =
                                    Pair(abnormalOffsetWindow!!.first, abnormalOffsetWindow!!.second)

                                val (finalConsumerOffset, fetchedRecords, subsequentAbnormalOffsetWindow, readTombstoneStatus) = standardRead(
                                    fileChannel,
                                    consumerOffset,
                                    producerOffset
                                )
                                records.addAll(fetchedRecords)
                                consumerOffset = finalConsumerOffset
                                abnormalOffsetWindow = subsequentAbnormalOffsetWindow

                                // It is of course possible that there are back-to-back hard transition failures,
                                // and this is reflected in the `originalAbnormalOffsetWindow` vs. `subsequentAbnormalOffsetWindow`
                                val result = FetchSuccess.HardTransition(
                                    originalAbnormalOffsetWindow.first,
                                    originalAbnormalOffsetWindow.second,
                                    readTombstoneStatus != ReadTombstoneStatus.RecordsAfterTombstone
                                )
                                right(result)
                            }
                        )
                }
            )
        }
    }

    private fun onHardTransitionCleanup(transition: FetchSuccess.HardTransition) {

        val subrecordLengthMetaLengthSum = transition.abnormalOffsetWindowStop - transition.abnormalOffsetWindowStart
        getConsumedSegmentChannel().use { channel ->
            var lock: FileLock?
            do {
                lock = channel.tryLock(transition.abnormalOffsetWindowStart, subrecordLengthMetaLengthSum, false)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            val recordBuffer = ByteBuffer.allocate(subrecordLengthMetaLengthSum.toInt()) //Saftey!
            channel.position(transition.abnormalOffsetWindowStart)
            channel.read(recordBuffer)
            val topic = getTopicOnPossiblyTruncatedMessage(recordBuffer)

            if (topic.equals(ControlTopic.INVALID_MESSAGE)) {
                logger.info("Hard transition record cleaned by another consumer")
            } else {
                val subrecordLength = hardTransitionSubrecordLength(subrecordLengthMetaLengthSum.toInt())
                // Matching the questionable convention in producer, should be changed when the producer changes
                val encodedTotalLength = subrecordLength.toVlqEncoding()

                val cleanupRecord = ByteBuffer.allocate(1 + encodedTotalLength.capacity() + subrecordLength)
                cleanupRecord.put(0xFF.toByte())
                cleanupRecord.put(encodedTotalLength)
                cleanupRecord.put(ControlTopic.INVALID_MESSAGE.value.size.toVlqEncoding())
                cleanupRecord.put(ControlTopic.INVALID_MESSAGE.value)
                cleanupRecord.rewind()

                channel.position(transition.abnormalOffsetWindowStart)
                channel.write(cleanupRecord)
            }
        }

        if (transition.recordsAfterTombstone) {
            reterminateSegment()
        }
    }

    private fun reterminateSegment() {
        getConsumedSegmentChannel().use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize - 5, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay)
                }
            } while (lock == null)
            val recordBuffer = ByteBuffer.allocate(5)
            channel.read(recordBuffer)
            getRecord(recordBuffer).onRight { (record, _) ->
                if (record.topic.contentEquals(ControlTopic.SEGMENT_TOMBSTONE.value)) {
                    logger.info("Segment reterminated by another consumer")
                    return
                }
            }

            channel.position(fileSize)
            channel.write(ByteBuffer.wrap(TOMBSTONE_RECORD))
        }
    }

    fun getStepCount(offsetDelta: Long) = (if (offsetDelta % readBufferSize == 0L) {
        (offsetDelta / readBufferSize)
    } else {
        (offsetDelta / readBufferSize) + 1
    }).toInt()

    private fun standardRead(
        fileChannel: FileChannel,
        startingReadOffset: Long,
        producerOffset: Long
    ): StandardRead {
        // When I transitioned to returning values rather than carrying out side effects I needed to decide how to best
        // name the return variables that would be mapped to fields by the caller and I used a `read` as a prefix
        var readOffset = startingReadOffset
        val steps = getStepCount(producerOffset - readOffset)

        val readRecords = ArrayList<PuroRecord>()
        var readAbnormalOffsetWindow: Pair<Long, Long>? = null

        var lastAbnormality: GetRecordAbnormality? = null
        for (step in 0..<steps) {
            val isLastBatch = (step == steps - 1)

            readBuffer.clear()
            if (isLastBatch) readBuffer.limit((producerOffset % readBufferSize).toInt()) //Saftey!

            fileChannel.read(readBuffer)
            readBuffer.flip()

            val (batchRecords, offsetChange, abnormality) = getRecords(
                readBuffer,
                0,
                if (isLastBatch) {
                    producerOffset % readBufferSize
                } else {
                    readBufferSize.toLong()
                },
                subscribedTopics,
                logger,
                isEndOfFetch = isLastBatch
            )
            lastAbnormality = abnormality

            readRecords.addAll(batchRecords)
            readOffset += offsetChange

            if (isLastBatch && abnormality == GetRecordAbnormality.Truncation) {
                readAbnormalOffsetWindow = consumerOffset to producerOffset
            } else if (abnormality == GetRecordAbnormality.RecordsAfterTombstone ||
                abnormality == GetRecordAbnormality.StandardTombstone && step < (steps - 1)
            ) {
                lastAbnormality = abnormality
                break
            }
        }

        return StandardRead(
            readOffset,
            readRecords,
            readAbnormalOffsetWindow,
            when (lastAbnormality) {
                is GetRecordAbnormality.StandardTombstone -> ReadTombstoneStatus.StandardTombstone
                is GetRecordAbnormality.RecordsAfterTombstone -> ReadTombstoneStatus.RecordsAfterTombstone
                else -> ReadTombstoneStatus.NotTombstoned
            }
        )
    }

    private fun getConsumedSegmentChannel(): FileChannel =
        FileChannel.open(getConsumedSegmentPath(), StandardOpenOption.READ)

    private fun getConsumedSegmentPath(): Path {
        val segment = getSegmentPath(streamDirectory, consumedSegmentOrder)
            ?: throw RuntimeException("This should never happen: non-existent segment requested for $consumedSegmentOrder")
        return segment
    }

    // Consumer lock block: returns `false` if end-of-fetch abnormality
    private fun <T> withConsumerLock(position: Long, size: Long, block: (FileChannel) -> T): T {
        getConsumedSegmentChannel().use { channel ->
            var lock: FileLock?
            do {
                lock = channel.tryLock(position, size, true)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            return block(channel)
        }
    }
}
