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
import java.util.concurrent.LinkedBlockingQueue


sealed class ConsumerError {
    class FailingCrc : ConsumerError()
    class NoReminingBuffer : ConsumerError()
    class HardTransitionFailure : ConsumerError()
}

sealed class FetchSuccess() {
    class CleanFetch() : FetchSuccess()
    class HardTransition(val abnormalOffsetWindowStart: Long, val abnormalOffsetWindowStop: Long) : FetchSuccess()
}

typealias ConsumerResult<R> = Either<ConsumerError, R>

fun getRecord(byteBuffer: ByteBuffer): ConsumerResult<Pair<PuroRecord, Int>> {
    if (!byteBuffer.hasRemaining()) {
        return left(ConsumerError.NoReminingBuffer())
    }
    val start = byteBuffer.position()
    val expectedCrc = byteBuffer.get()
    val (encodedTotalLength, _, crc1) = byteBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
    val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
    val (key, crc5) = byteBuffer.getSubsequence(keyLength)
    val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        right(PuroRecord(topic, key, value) to (byteBuffer.position() - start))
    } else left(ConsumerError.FailingCrc())
}

fun getRecords(
    byteBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long,
    listenedTopics: List<String>,
    logger: Logger,
    isEndOfFetch: Boolean = false
): Triple<List<PuroRecord>, Long, Boolean> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var abnormality = false // Only matters if end-of-fetch
    byteBuffer.position(initialOffset.toInt()) //Saftey issue
    byteBuffer.limit(finalOffset.toInt()) //Saftey issue
    logger.info("Initial: ${initialOffset}/${finalOffset.toInt()}")
    logger.info(byteBuffer.remaining().toString())

    //logger.info("Initial offset: $offset, remaining: ${byteBuffer.remaining()}")
    while (byteBuffer.hasRemaining()) {
        val expectedCrc = byteBuffer.get()

        //val (encodedTotalLength, encodedTotalLengthBitCount, crc1) = byteBuffer.fromVlq()
        val lengthData = byteBuffer.readSafety()?.fromVlq()

        //val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
        val topicLengthData = byteBuffer.readSafety()?.fromVlq()

        //TODO optimisation: as soon as the length and topic are known, skip message if not in `listenedTopics`

        //val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
        val topicMetadata = if (topicLengthData != null) {
            byteBuffer.readSafety()?.getEncodedString(topicLengthData.first)
        } else null

        //val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
        val keyMetdata = byteBuffer.readSafety()?.fromVlq()

        // val (key, crc5) = byteBuffer.getSubsequence(keyLength)
        val keyData = if (keyMetdata != null) {
            byteBuffer.readSafety()?.getSubsequence(keyMetdata.first)
        } else null

        // val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)
        val valueData = if (lengthData != null && topicLengthData != null && keyMetdata != null) {
            byteBuffer.readSafety()
                ?.getSubsequence(lengthData.first - topicLengthData.second - topicLengthData.first - keyMetdata.second - keyMetdata.first)
        } else null

        // The else branch isn't advancing the offset because it is possible that this is the next batch
        // TODO: while the reasoning above is sound, but we now have a baked in assumption that the only time
        // TODO: ...we will have bad messages is for the outside of fetches. Should throw if this is not the case,
        // TODO: ...this is now maked as 'Fetch interior failures' in the readme
        if (lengthData != null && topicLengthData != null && topicMetadata != null && keyMetdata != null && keyData != null && valueData != null) {
            val (_, encodedTotalLengthBitCount, crc1) = lengthData
            val (topicLength, topicLengthBitCount, crc2) = topicLengthData
            val (topic, crc3) = topicMetadata
            val (keyLength, keyLengthBitCount, crc4) = keyMetdata
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

            val subrecordLength = topicLengthBitCount + topicLength + keyLengthBitCount + keyLength + value.capacity()
            val totalLength = 1 + encodedTotalLengthBitCount + subrecordLength

            if (expectedCrc == actualCrc && listenedTopics.contains(topic)) {
                records.add(PuroRecord(topic, key, value))
            }

            // These are necessarily 'interior' messages.
            offset += totalLength
        } else {
            abnormality = true
        }
    }
    //logger.info("Final Offset: $offset")
    //logger.info("Records count: ${records.size}")
    //val a = finalOffset - initialOffset
    //val b = offset
    //logger.info("Final: ${a}/${b}")
    return Triple(records, offset, if (isEndOfFetch) abnormality else false)
}

class PuroConsumer(
    val streamDirectory: Path,
    val topics: List<String>,
    val logger: Logger,
    val onMessage: (PuroRecord) -> Unit, //TODO add logger
) : Runnable {
    companion object {
        const val BUFFSIZE = 8192
    }

    var activeSegmentPath = getActiveSegmentPath(streamDirectory)
    var consumerOffset = 0L //First byte offset that hasn't been read
    val readBuffer = ByteBuffer.allocate(BUFFSIZE)
    var abnormalOffsetWindow: Pair<Long, Long>? = null
    var activeSegmentChangeQueue = LinkedBlockingQueue<Long>()

    //fun stopListening() = watcher?.close()

    private val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(streamDirectory)
        .listener { event: DirectoryChangeEvent ->
            when (event.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> {
                    if (event.path() == activeSegmentPath) {
                        activeSegmentChangeQueue.put(Files.size(activeSegmentPath))
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
                val producerOffset = activeSegmentChangeQueue.take()
                //logger.info("New producer offset $producerOffset")
                onActiveSegmentChange(producerOffset)
            }
        }.start()
    }

    private fun ArrayList<PuroRecord>.onMessages() {
        //TODO: remove the filter once the getMessage optimisation taken care of?
        this.filter { r -> topics.contains(r.topic) }
            .forEach { r -> onMessage(r) }
    }

    fun onActiveSegmentChange(producerOffset: Long) {
        val records = ArrayList<PuroRecord>()

        // TODO: if the lambda is broken out it will be much easier to test
//        logger.info("Consumer offset: $consumerOffset, Producer offset: $producerOffset")
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

    fun fetch(
        fileChannel: FileChannel,
        records: ArrayList<PuroRecord>,
        producerOffset: Long
    ): ConsumerResult<FetchSuccess> {
        if (abnormalOffsetWindow == null) {
            standardRead(fileChannel, records, producerOffset)
            return right(FetchSuccess.CleanFetch())
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
                    right(FetchSuccess.CleanFetch())
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
                                Either.Left<ConsumerError, FetchSuccess>(ConsumerError.HardTransitionFailure())
                            },
                            ifRight = { hardProducerTransitionResult ->
                                consumerOffset += hardProducerTransitionResult.second
                                val result = FetchSuccess.HardTransition(
                                    abnormalOffsetWindow!!.first,
                                    abnormalOffsetWindow!!.second
                                )
                                abnormalOffsetWindow = null
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

    fun getStepCount(offsetDelta: Long) = (if (offsetDelta % BUFFSIZE == 0L) {
        (offsetDelta / BUFFSIZE)
    } else {
        (offsetDelta / BUFFSIZE) + 1
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
                topics,
                logger,
                isEndOfFetch = isLastBatch
            )

            records.addAll(batchRecords)
            consumerOffset = nextOffset

            if (isLastBatch && abnormality) {
                abnormalOffsetWindow = consumerOffset to producerOffset
            }
        }
        //logger.info("286: consumer offset $consumerOffset, producer offset $producerOffset, $steps")
    }

    private fun getActiveSegmentChannel(): FileChannel {
        // Setting this as a side effect, not super jazzed about this
        activeSegmentPath = getActiveSegmentPath(streamDirectory)
        return FileChannel.open(activeSegmentPath, StandardOpenOption.READ)
    }

    // Consumer lock block: returns `false` if end-of-fetch abnormality
    private fun <T> withConsumerLock(position: Long, size: Long, block: (FileChannel) -> T): T {
        // There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
        // the active segment could change, this has been marked on the README as 'Active segment transition race
        // condition handling'; Best way may be to read if 'tombstone'
        //logger.info("position: $position, size: $size")
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
