import PuroProducer.Companion.retryDelay
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
import kotlin.math.log


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

sealed class FetchSuccess() {
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

fun getRecord(byteBuffer: ByteBuffer): ConsumerResult<Pair<PuroRecord, Int>> {
    if (!byteBuffer.hasRemaining()) {
        return left(ConsumerError.NoRemainingBuffer)
    }
    val start = byteBuffer.position()
    val expectedCrc = byteBuffer.get()
    val (encodedTotalLength, _, crc1) = byteBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
    val (topic, crc3) = byteBuffer.getArraySlice(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
    val (key, crc5) = byteBuffer.getBufferSlice(keyLength)
    val (value, crc6) = byteBuffer.getBufferSlice(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        right(PuroRecord(topic, key, value) to (byteBuffer.position() - start))
    } else left(ConsumerError.FailingCrc)
}

fun isRelevantTopic(
    topic: ByteArray,
    subscribedTopics: List<ByteArray>,
): Boolean = subscribedTopics.any { it.contentEquals(topic) } || ControlTopic.entries.toTypedArray()
    .any { it.value.contentEquals(topic) }

fun getRecords(
    byteBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long,
    subscribedTopics: List<ByteArray>,
    logger: Logger,
    isEndOfFetch: Boolean = false
): Triple<List<PuroRecord>, Long, GetRecordAbnormality?> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var truncationAbnormality = false // Only matters if end-of-fetch

    byteBuffer.position(initialOffset.toInt()) //Saftey issue
    byteBuffer.limit(finalOffset.toInt()) //Saftey issue

    while (byteBuffer.hasRemaining()) {
        val expectedCrc = byteBuffer.get()

        //val (encodedTotalLength, encodedTotalLengthBitCount, crc1) = byteBuffer.fromVlq()
        val lengthData = byteBuffer.readSafety()?.fromVlq()
        //val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
        val topicLengthData = byteBuffer.readSafety()?.fromVlq()
        //val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
        val topicMetadata = if (topicLengthData != null) {
            byteBuffer.readSafety()?.getArraySlice(topicLengthData.first)
        } else null

        //TODO: Subject this to microbenchmarks, not sure if this actually matters
        if (topicMetadata == null || !isRelevantTopic(topicMetadata.first, subscribedTopics)) {
            if (lengthData != null && (1 + lengthData.second + lengthData.first) <= byteBuffer.remaining()) {
                offset += (1 + lengthData.second + lengthData.first)
                continue
            }
        }
        //val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
        val keyMetdata = byteBuffer.readSafety()?.fromVlq()
        // val (key, crc5) = byteBuffer.getSubsequence(keyLength)
        val keyData = if (keyMetdata != null) {
            byteBuffer.readSafety()?.getBufferSlice(keyMetdata.first)
        } else null
        // val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)
        val valueData = if (lengthData != null && topicLengthData != null && keyMetdata != null) {
            byteBuffer.readSafety()
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
                val abnormality = if (isEndOfFetch || byteBuffer.hasRemaining()) {
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

fun hardTransitionSubrecordLength(absoluteMessageSize: Int): Int {
    //messageSize = 1 + capacity(vlq(subrecordLength)) + subrecordLength
    var subrecordLength = absoluteMessageSize - 1

    while (absoluteMessageSize != 1 + ceilingDivision(
            Int.SIZE_BITS - subrecordLength.countLeadingZeroBits(),
            7
        ) + subrecordLength
    ) {
        var a =1 + ceilingDivision(
            Int.SIZE_BITS - subrecordLength.countLeadingZeroBits(),
            7
        ) + subrecordLength
        subrecordLength--
    }
    return subrecordLength
}

class PuroConsumer(
    val streamDirectory: Path,
    serialisedTopicNames: List<String>,
    val logger: Logger,
    val buffsize: Int = 8192,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) : Runnable {
    //TODO make this configurable:
    var consumedSegment = getActiveSegment(streamDirectory)
    var consumerOffset = 0L //First byte offset that hasn't been read
    val readBuffer = ByteBuffer.allocate(buffsize)
    var abnormalOffsetWindow: Pair<Long, Long>? = null
    var currentConsumerLocked = false

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

    var segmentChangeQueue = PriorityBlockingQueue(0, compareConsumerSegmentPair)
    var subscribedTopics = serialisedTopicNames.map { it.toByteArray() }

    //fun stopListening() = watcher?.close()

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    //TODO cache this, no reason to recompute
                    val currentSegmentOrder = getSegmentOrder(consumedSegment)
                    val incomingSegmentOrder = getSegmentOrder(event.path())

                    if (event.path() == consumedSegment) {
                        segmentChangeQueue.offer(ConsumerSegmentEvent(Files.size(consumedSegment), currentSegmentOrder))
                    } else if (incomingSegmentOrder > currentSegmentOrder) {
                        segmentChangeQueue.offer(
                            ConsumerSegmentEvent(
                                Files.size(consumedSegment),
                                incomingSegmentOrder
                            )
                        )
                    } else {
                        logger.warn("Modify event for non-consumed path ${event.path()} recorded, possible damaged data integrity")
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
        Thread { watcher?.watch() }.start()
        Thread {
            while (true) {
                val (producerOffset, incomingSegmentOrder) = segmentChangeQueue.take()
                val consumedSegmentOrder = getSegmentOrder(consumedSegment)

                if (currentConsumerLocked) {
                    logger.error("Ignored ConsumerSegmentEvent: segment locked, current order $incomingSegmentOrder")
                } else if (incomingSegmentOrder == consumedSegmentOrder) {
                    onConsumedSegmentAppend(producerOffset)
                } else if (incomingSegmentOrder == consumedSegmentOrder + 1) {
                    val nextSegment = getSegment(streamDirectory, incomingSegmentOrder)
                    if (nextSegment != null) {
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

    fun onHardTransitionCleanup(transition: FetchSuccess.HardTransition) {
        //TODO: Write a message with a zero'd CRC bit, zero topic and key lengths, with a message length to match the gap
        //TODO: Re-acquire a read lock and continue on as normal
        //TODO: After the lock is acquired, call a function outside of the class
        if (transition.recordsAfterTombstone) {
            reterminateSegment()
        }
    }

    fun reterminateSegment() {
        //TODO
    }


    fun onConsumedSegmentTransition() {
        //TODO
    }

    fun getStepCount(offsetDelta: Long) = (if (offsetDelta % buffsize == 0L) {
        (offsetDelta / buffsize)
    } else {
        (offsetDelta / buffsize) + 1
    }).toInt()

    sealed class ReadTombstoneStatus {
        data object NotTombstoned : ReadTombstoneStatus()
        data object RecordsAfterTombstone : ReadTombstoneStatus()
        data object StandardTombstone : ReadTombstoneStatus()

    }

    data class StandardRead(
        val finalConsumerOffset: Long,
        val fetchedRecords: ArrayList<PuroRecord>,
        val abnormalOffsetWindow: Pair<Long, Long>?,
        val recordsAfterTombstone: ReadTombstoneStatus,
    )

    fun standardRead(
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
            readBuffer.clear()
            fileChannel.read(readBuffer)
            readBuffer.flip()
            val isLastBatch = (step == steps - 1)
            val (batchRecords, nextReadConsumerOffset, abnormality) = getRecords(
                readBuffer,
                readOffset,
                producerOffset,
                subscribedTopics,
                logger,
                isEndOfFetch = isLastBatch
            )
            lastAbnormality = abnormality

            readRecords.addAll(batchRecords)
            readOffset = nextReadConsumerOffset

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

    private fun getActiveSegmentChannel(): FileChannel {
        // Setting this as a side effect, not super jazzed about this
        //observedActiveSegment = getActiveSegment(streamDirectory)
        return FileChannel.open(consumedSegment, StandardOpenOption.READ)
    }

    // Consumer lock block: returns `false` if end-of-fetch abnormality
    private fun <T> withConsumerLock(position: Long, size: Long, block: (FileChannel) -> T): T {
        getActiveSegmentChannel().use { channel ->
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
