import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import org.slf4j.Logger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.PriorityBlockingQueue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

sealed class ConsumerStartPoint {
    data object StreamBeginning : ConsumerStartPoint()
    data object Latest : ConsumerStartPoint()
}

sealed class ConsumerError {
    data object FailingCrc : ConsumerError()
    data object NoRemainingBuffer : ConsumerError()
    data object HardTransitionFailure : ConsumerError()
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
    val abnormality: GetRecordsAbnormality?
)

private sealed class FetchSuccess() {
    data object CleanFetch : FetchSuccess()
    data object CleanSegmentClosure : FetchSuccess()
    data object RecordsAfterTombstone : FetchSuccess()
}

typealias ConsumerResult<R> = Either<ConsumerError, R>

fun getRecord(recordBuffer: ByteBuffer): ConsumerResult<Pair<PuroRecord, Int>> {
    if (!recordBuffer.hasRemaining()) {
        return left(ConsumerError.NoRemainingBuffer)
    }
    val start = recordBuffer.position()
    val expectedCrc = recordBuffer.get()
    val (encodedSubrecordLength, _, crc1) = recordBuffer.fromVlq()
    val (topicLength, topicLengthByteCount, crc2) = recordBuffer.fromVlq()
    val (topic, crc3) = recordBuffer.getArraySlice(topicLength)
    val (keyLength, keyLengthByteCount, crc4) = recordBuffer.fromVlq()
    val (key, crc5) = recordBuffer.getBufferSlice(keyLength)
    val (value, crc6) = recordBuffer.getBufferSlice(encodedSubrecordLength - topicLengthByteCount - topicLength - keyLengthByteCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        right(PuroRecord(topic, key, value) to (recordBuffer.position() - start))
    } else left(ConsumerError.FailingCrc)
}

fun isRelevantTopic(
    topic: ByteArray,
    subscribedTopics: List<ByteArray>,
    otherIncludedTopics: List<ControlTopic>,
): Boolean =
    subscribedTopics.any { it.contentEquals(topic) } || (otherIncludedTopics.any { it.value.contentEquals(topic) })

class Consumer(
    val streamDirectory: Path,
    serialisedTopicNames: List<String>,
    val logger: Logger,
    val readBufferSize: Int = 8192,
    val startPoint: ConsumerStartPoint = ConsumerStartPoint.Latest,
    val onMessage: (PuroRecord, Logger) -> Unit,
) : Runnable {
    companion object {
        private var defaultRetryDelay = 1.seconds.toJavaDuration()
        private var maximumRetryDelay = 10.milliseconds.toJavaDuration()

        // I don't have very good intuition on this one, should be benchmarked
        private val queueCapacity = 10_000
    }

    //TODO make this configurable:
    //var _consumedSegment = getActiveSegment(streamDirectory)

    //This is a default and there is probably a better way to handle this given `watcherPreInitialisation` I guess
    //Really this block of three fields are more interesting in `watcherPreInitialisation`
    private var consumedSegmentOrder = getLowestSegmentOrder(streamDirectory)
    private var consumerOffset = 0L
    private var currentConsumerLocked = false
    private var consumeState: ConsumeState = ConsumeState.Standard
    private var retryDelay = defaultRetryDelay


    val readBuffer = ByteBuffer.allocate(readBufferSize)
    private var abnormalOffsetWindow: Pair<Long, Long>? = null

    // PriorityBlockingQueue javadoc:
    // "Operations on this class make no guarantees about the ordering of elements with equal priority.
    // If you need to enforce an ordering, you can define custom classes or comparators that use a secondary key to
    // break ties in primary priority values"
    private val compareConsumerSegmentPair =
        Comparator<ConsumerSegmentEvent> { p1, p2 ->
            return@Comparator (if (p1.segmentOrder != p2.segmentOrder) {
                p1.segmentOrder - p2.segmentOrder
            } else {
                (p1.offset - p2.offset).toInt()
            })
        }

    private var segmentChangeQueue = PriorityBlockingQueue(queueCapacity, compareConsumerSegmentPair)
    private var subscribedTopics = serialisedTopicNames.map { it.toByteArray() }

    private fun stopListening() = watcher?.close()

    /*
        Before the watch service is started two preconditions need to be met
        1) A segment to poll has to actually exist
        2) An appropriate segment to consume and consumer offset have to be set based off of `startPoint`
     */
    private fun watcherPreInitialisation() {
        while (getLowestSegmentOrder(streamDirectory) == -1) {
            Thread.sleep(retryDelay) // Should eventually give up
        }
        consumedSegmentOrder = when (startPoint) {
            // See README 'Stale and spurious segment problems'
            is ConsumerStartPoint.StreamBeginning -> getLowestSegmentOrder(streamDirectory)
            // See README 'raggged start'
            is ConsumerStartPoint.Latest -> getHighestSegmentOrder(streamDirectory, readBuffer, retryDelay, logger)
        }

        consumedSegmentOrder = getLowestSegmentOrder(streamDirectory)
        // The answer to 'what happens if something is deleted at the worst time' is not very good here
        val path = getSegmentPath(streamDirectory, consumedSegmentOrder)
        path
            ?: throw RuntimeException("Illegal state: consumed segment order of $consumedSegmentOrder didn't have a path on startup")
        // I don't think double events are risked by this, but it's taken care of by the `onConsumedSegmentAppend`
        segmentChangeQueue.put(ConsumerSegmentEvent(Files.size(path), consumedSegmentOrder))
    }

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            // Diagnostic logs

            //TODO cache this, no reason to recompute
            //val currentSegmentOrder = getSegmentOrder(consumedSegment)
            val incomingSegmentOrder = getSegmentOrder(event.path())

            if (incomingSegmentOrder == -1) {
                logger.info("Incoming non-segment ${event.eventType()} event for ${event.path()}")
                return@listener
            } else {
                logger.info("Incoming ${event.eventType()} event for ${event.path()}")
            }
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    if (incomingSegmentOrder == consumedSegmentOrder) {
                        segmentChangeQueue.offer(
                            ConsumerSegmentEvent(
                                Files.size(getConsumedSegmentPath()),
                                consumedSegmentOrder
                            )
                        )
                    } else if (incomingSegmentOrder > consumedSegmentOrder) {
                        val incomingSegment = getSegmentPath(streamDirectory, incomingSegmentOrder)
                        if (incomingSegment == null) {
                            logger.error("Phantom incoming segment of order $incomingSegmentOrder")
                        } else {
                            segmentChangeQueue.offer(
                                ConsumerSegmentEvent(Files.size(incomingSegment), incomingSegmentOrder)
                            )
                        }
                    } else {
                        logger.warn(
                            """Modify event for segment $incomingSegmentOrder recorded with 
                            $consumedSegmentOrder as consumed order, possible damaged data integrity""".trimIndent()
                                .replace("\n", " ")
                        )
                    }
                }

                DirectoryChangeEvent.EventType.CREATE -> {}
                DirectoryChangeEvent.EventType.DELETE -> {
                } //Log something probably
                DirectoryChangeEvent.EventType.OVERFLOW -> {
                    logger.warn("Overflow event on segment $incomingSegmentOrder")
                }

                else -> {
                    logger.error("Illegal DirectoryChangeEvent ${event.eventType()} received")
                } // This should never happen
            }
        }
        .build()

    override fun run() {
        logger.info("Starting Consumer")
        Thread {
            watcherPreInitialisation()
            watcher?.watch()
        }.start()
        logger.info("Consumer Directory Watcher Configured")

        catchupThread().start()

        Thread {
            while (true) {
                val (producerOffset, incomingSegmentOrder) = segmentChangeQueue.take()

                if (currentConsumerLocked and (incomingSegmentOrder == consumedSegmentOrder)) {
                    logger.error("Ignored ConsumerSegmentEvent: segment locked, current order $consumedSegmentOrder")
                } else if (incomingSegmentOrder == consumedSegmentOrder) {
                    logger.info("Incoming producer offset: $producerOffset")
                    onConsumedSegmentAppend(producerOffset)
                } else if (currentConsumerLocked and (incomingSegmentOrder == consumedSegmentOrder + 1)) {
                    // The priority queue will ensure all records of order _N_ will be received before all records of
                    // order _N+1_
                    val nextSegment = getSegmentPath(streamDirectory, consumedSegmentOrder + 1)
                    if (nextSegment != null) {
                        consumedSegmentOrder++
                        currentConsumerLocked = false
                        onConsumedSegmentAppend(producerOffset)
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
        this.forEach { r -> onMessage(r, logger) }
    }

    private fun onConsumedSegmentAppend(producerOffset: Long) {
        val records = ArrayList<PuroRecord>()

        if (producerOffset <= consumerOffset) {
            logger.warn("Consumer received out-of-date producer offset $producerOffset while at consumer offset $consumerOffset")
        }

        val fetchResult = withConsumerLock(consumerOffset, producerOffset - consumerOffset) { fileChannel ->
            fetch(fileChannel, records, producerOffset)
        }

        fetchResult.onRight {
            when (it) {
                null, is GetRecordsAbnormality.Truncation -> records.onMessages()
                //logger.info(if(consumeState is ConsumeState.Large) { "large" } else {"small"})

                is GetRecordsAbnormality.StandardTombstone, GetRecordsAbnormality.RecordsAfterTombstone -> {
                    currentConsumerLocked = true
                    records.onMessages()
                }
                // The `consumedSegmentOrder` _really_ should not have changed between these two points
                is GetRecordsAbnormality.LowSignalBit -> segmentChangeQueue.put(
                    ConsumerSegmentEvent(
                        producerOffset,
                        consumedSegmentOrder
                    )
                )
            }
        }.onLeft {
            //TODO an actually acceptable logging message
            throw RuntimeException(it.toString())
        }
    }

    private fun standardFetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<GetRecordsAbnormality?> {
        val (finalConsumerOffset, fetchedRecords, readAbnormalOffsetWindow, abnormality) = standardRead(
            fileChannel,
            consumerOffset,
            producerOffset,
        )
        records.addAll(fetchedRecords)
        consumerOffset = finalConsumerOffset
        abnormalOffsetWindow = readAbnormalOffsetWindow

        return right(abnormality)
    }

    private fun fetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<GetRecordsAbnormality?> {
        if (abnormalOffsetWindow == null) {
            return standardFetch(fileChannel, records, producerOffset)
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
                    return standardFetch(fileChannel, records, producerOffset)
                },
                ifLeft = {
                    //readBuffer.rewind()
                    //val bufferOffset = (abnormalOffsetWindow!!.second - abnormalOffsetWindow!!.first).toInt()
                    //readBuffer.position(bufferOffset + 1)
                    Either.Left<ConsumerError, GetRecordsAbnormality?>(ConsumerError.HardTransitionFailure)
                }
            )
        }
    }

    private fun standardRead(
        fileChannel: FileChannel,
        startingReadOffset: Long,
        producerOffset: Long
    ): StandardRead {
        // When I transitioned to returning values rather than carrying out side effects I needed to decide how to best
        // name the return variables that would be mapped to fields by the caller and I used a `read` as a prefix
        var readOffset = startingReadOffset
        logger.info("Starting read offset $readOffset")

        val readRecords = ArrayList<PuroRecord>()
        var readAbnormalOffsetWindow: Pair<Long, Long>? = null
        var abnormality: GetRecordsAbnormality? = null
        var isLastBatch = false
        var isPossibleLastBatch: Boolean

        while (!isLastBatch) {
            isPossibleLastBatch = (producerOffset - readOffset) <= readBufferSize

            readBuffer.clear()
            if (isPossibleLastBatch) {
                readBuffer.limit(((producerOffset - readOffset) % readBufferSize).toInt()) //Saftey!
            }

            fileChannel.position(readOffset)
            fileChannel.read(readBuffer)
            readBuffer.flip()

            when (consumeState) {
                is ConsumeState.Standard -> {
                    val getSignalRecordsResult = getRecords(
                        readBuffer,
                        0,
                        if (isPossibleLastBatch) {
                            (producerOffset - readOffset) % readBufferSize
                        } else {
                            readBufferSize.toLong()
                        },
                        subscribedTopics,
                        isEndOfFetch = isPossibleLastBatch,
                        logger
                    )

                    when (getSignalRecordsResult) {
                        is GetRecordsResult.Success -> {
                            readRecords.addAll(getSignalRecordsResult.records)
                            logger.info("Standard offset change ${getSignalRecordsResult.offset}")
                            readOffset += getSignalRecordsResult.offset
                        }

                        is GetRecordsResult.StandardAbnormality -> {
                            abnormality = getSignalRecordsResult.abnormality
                            readRecords.addAll(getSignalRecordsResult.records)
                            // Talk as of 7ba7441 lastAbnormality wasn't used where it could have been, possible some subtle bugs here
                            if (isPossibleLastBatch && abnormality == GetRecordsAbnormality.Truncation) {
                                readAbnormalOffsetWindow = consumerOffset to producerOffset
                            } else if (abnormality == GetRecordsAbnormality.RecordsAfterTombstone ||
                                abnormality == GetRecordsAbnormality.StandardTombstone && !isPossibleLastBatch
                            ) {
                                break
                            } else if (abnormality == GetRecordsAbnormality.LowSignalBit) {
                                logger.info("Low signal bit at ${readOffset + getSignalRecordsResult.offset}")
                                val incrementedDelay = retryDelay.toMillis() * 2
                                if (incrementedDelay < maximumRetryDelay.toMillis()) {
                                    retryDelay = incrementedDelay.milliseconds.toJavaDuration()
                                }
                            }
                            logger.info("Standard record offset change ${getSignalRecordsResult.offset}")
                            readOffset += getSignalRecordsResult.offset
                        }

                        is GetRecordsResult.LargeRecordStart -> {
                            logger.info("Consumer state transition to large")
                            consumeState = ConsumeState.Large(
                                arrayListOf(getSignalRecordsResult.largeRecordFragment),
                                getSignalRecordsResult.targetBytes
                            )
                            readOffset += getSignalRecordsResult.partialReadOffset
                        }
                    }
                }

                is ConsumeState.Large -> {
                    // TODO consider if proving this type via thread saftey actually matters
                    val currentConsumeState = consumeState as ConsumeState.Large
                    // Check if using position is relevant here
                    val getLargeSignalRecordsResult = getLargeSignalRecords(
                        currentConsumeState.targetBytes,
                        currentConsumeState.largeRecordFragments.sumOf { it.position() }
                            .toLong(), //! assumes buffers not rewound
                        readBuffer,
                        0,
                        if (isPossibleLastBatch) {
                            (producerOffset - readOffset) % readBufferSize
                        } else {
                            readBufferSize.toLong()
                        }
                    )

                    currentConsumeState.largeRecordFragments.add(getLargeSignalRecordsResult.byteBuffer)
                    val offsetChange =
                        getLargeSignalRecordsResult.byteBuffer.limit()  //Talk: assumes buffers not rewound, also kinda annoying bug found here
                    readOffset += offsetChange
                    logger.info("Large record offset change $offsetChange to $readOffset")

                    abnormality = deserialiseLargeReadWithAbnormalityTracking(
                        getLargeSignalRecordsResult,
                        currentConsumeState,
                        readRecords,
                        abnormality
                    )
                }
            }
            isLastBatch = isPossibleLastBatch && readOffset == producerOffset
        }
        logger.info("Final read offset $readOffset")

        return StandardRead(
            readOffset,
            readRecords,
            readAbnormalOffsetWindow,
            abnormality
        )
    }

    private fun deserialiseLargeReadWithAbnormalityTracking(
        getLargeSignalRecordsResult: GetLargeSignalRecordResult,
        currentConsumeState: ConsumeState.Large,
        readRecords: ArrayList<PuroRecord>,
        lastAbnormality: GetRecordsAbnormality?
    ): GetRecordsAbnormality? {
        var abnormality = lastAbnormality
        if (getLargeSignalRecordsResult is GetLargeSignalRecordResult.LargeRecordEnd) {
            //val bytes = currentConsumeState.largeRecordFragments.map { it.array() }.reduce { acc, any -> acc + any }
            //logger.info("[Consumer] multibyte: ${bytes.joinToString { it.toString() }}")
            //logger.info("[Consumer] multibyte message: ${bytes.decodeToString()}")
            when (val deserialisedLargeRead = deserialiseLargeRead(currentConsumeState, subscribedTopics)) {
                is DeserialiseLargeReadResult.Standard -> {
                    readRecords.add(deserialisedLargeRead.puroRecord)
                }

                is DeserialiseLargeReadResult.SegmentTombstone -> {
                    readRecords.add(deserialisedLargeRead.puroRecord)
                    abnormality = GetRecordsAbnormality.StandardTombstone
                }

                is DeserialiseLargeReadResult.IrrelevantTopic -> {
                    logger.info("Large message has irrelevant topic")
                }

                is DeserialiseLargeReadResult.CrcFailure -> logger.warn("CRC failure on large message")
            }
            logger.info("Consumer state transition to standard")
            consumeState = ConsumeState.Standard
        }
        return abnormality
    }

    private fun getConsumedSegmentChannel(): FileChannel =
        FileChannel.open(getConsumedSegmentPath(), StandardOpenOption.READ)

    private fun getConsumedSegmentPath(): Path {
        val segment = getSegmentPath(streamDirectory, consumedSegmentOrder)
            ?: throw RuntimeException("This should never happen: non-existent segment requested for $consumedSegmentOrder")
        return segment
    }

    //The directory watcher sometimes doesn't catch events right after the consumer is created
//Duplicate `segmentChangeQueue` events are harmless
    private fun catchupThread() = Thread {
        Thread.sleep(100)
        getConsumedSegmentChannel().use { channel ->
            var lock: FileLock? = null
            do {
                try {
                    lock = channel.tryLock(0, Long.MAX_VALUE, true)
                    if (lock == null) {
                        Thread.sleep(retryDelay) // Should eventually give up
                    }
                } catch (_: OverlappingFileLockException) {
                    logger.warn("Hit OverlappingFileLockException, should only happen when testing mutliple clients in same JVM")
                    Thread.sleep(retryDelay)
                }
            } while (lock == null)
            segmentChangeQueue.put(ConsumerSegmentEvent(channel.size(), consumedSegmentOrder))
        }
    }

    // Consumer lock block: returns `false` if end-of-fetch abnormality
    private fun <T> withConsumerLock(position: Long, size: Long, block: (FileChannel) -> T): T {
        getConsumedSegmentChannel().use { channel ->
            var lock: FileLock? = null
            do {
                try {
                    lock = channel.tryLock(position, size, true)
                    if (lock == null) {
                        Thread.sleep(retryDelay) // Should eventually give up
                    }
                } catch (_: OverlappingFileLockException) {
                    logger.warn("Hit OverlappingFileLockException, should only happen when testing mutliple clients in same JVM")
                    Thread.sleep(retryDelay)
                }
            } while (lock == null)
            return block(channel)
        }
    }
}
