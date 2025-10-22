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


sealed class ConsumerError {
    data object FailingCrc : ConsumerError()
    data object NoReminingBuffer : ConsumerError()
    data object HardTransitionFailure : ConsumerError()
}

sealed class GetRecordAbnormality {
    data object Truncation : GetRecordAbnormality()
    data object RecordsAfterTombstone : GetRecordAbnormality()
}

sealed class FetchSuccess() {
    data object CleanFetch : FetchSuccess()
    class HardTransition(val abnormalOffsetWindowStart: Long, val abnormalOffsetWindowStop: Long) : FetchSuccess()
}

typealias ConsumerResult<R> = Either<ConsumerError, R>

fun getRecord(byteBuffer: ByteBuffer): ConsumerResult<Pair<PuroRecord, Int>> {
    if (!byteBuffer.hasRemaining()) {
        return left(ConsumerError.NoReminingBuffer)
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

        //TODO optimisation: as soon as the length and topic are known, skip message if not in `listenedTopics`

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
            } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic) && !byteBuffer.hasRemaining()) {
                return Triple(records, offset, if (isEndOfFetch && byteBuffer.hasRemaining()) GetRecordAbnormality.RecordsAfterTombstone else null)
            }

            // These are necessarily 'interior' messages.
            offset += totalLength
        } else {
            truncationAbnormality = true
        }
    }

    return Triple(records, offset, if (isEndOfFetch && truncationAbnormality) GetRecordAbnormality.Truncation else null)
}

class PuroConsumer(
    streamDirectory: Path,
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
                        segmentChangeQueue.offer(ConsumerSegmentEvent(-1, incomingSegmentOrder))
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
                val (producerOffset) = segmentChangeQueue.take()
                // TODO the magic value of a zero consumer offset is kinda lame, would
                // TODO it be better to handle a null instead?
                if (producerOffset > 0) {
                    onConsumedSegmentAppend(producerOffset)
                } else {
                    onConsumedSegmentTransition()
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
                is FetchSuccess.CleanFetch -> records.onMessages()

                is FetchSuccess.HardTransition -> {
                    hardTransitionCleanup(it)
                    records.onMessages()
                }
            }
        }.onLeft {
            //TODO an actually acceptable logging message
            throw RuntimeException(it.toString())
        }
    }

    fun onConsumedSegmentTransition() {
        //TODO
    }

    fun fetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<FetchSuccess> {
        if (abnormalOffsetWindow == null) {
            standardRead(fileChannel, records, producerOffset)
            //TODO: check messages after tombstone
            return right(FetchSuccess.CleanFetch)
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
                    standardRead(fileChannel, records, producerOffset)
                    // No cleanup required
                    //TODO: check messages after tombstone
                    right(FetchSuccess.CleanFetch)
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
                                val result = FetchSuccess.HardTransition(
                                    abnormalOffsetWindow!!.first,
                                    abnormalOffsetWindow!!.second
                                )
                                abnormalOffsetWindow = null
                                //TODO: check messages after tombstone
                                standardRead(fileChannel, records, producerOffset)
                                right(result)
                            }
                        )
                },
            )
        }
    }

    fun hardTransitionCleanup(transition: FetchSuccess.HardTransition) {
        //TODO: Write a message with a zero'd CRC bit, zero topic and key lengths, with a message length to match the gap
        //TODO: Re-acquire a read lock and continue on as normal
        //TODO: After the lock is acquired, call a function outside of the class
    }

    fun getStepCount(offsetDelta: Long) = (if (offsetDelta % buffsize == 0L) {
        (offsetDelta / buffsize)
    } else {
        (offsetDelta / buffsize) + 1
    }).toInt()


    fun standardRead(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ) {
        val steps = getStepCount(producerOffset - consumerOffset)
        for (step in 0..<steps) {
            readBuffer.clear()
            fileChannel.read(readBuffer)
            readBuffer.flip()
            val isLastBatch = (step == steps - 1)
            val (batchRecords, nextOffset, abnormality) = getRecords(
                readBuffer,
                consumerOffset,
                producerOffset,
                subscribedTopics,
                logger,
                isEndOfFetch = isLastBatch
            )

            records.addAll(batchRecords)
            consumerOffset = nextOffset

            if (isLastBatch && abnormality == GetRecordAbnormality.Truncation) {
                abnormalOffsetWindow = consumerOffset to producerOffset
            }
        }
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
