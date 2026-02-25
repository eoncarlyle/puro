import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.time.Duration
import kotlin.experimental.and

// This is for common utilities between consumers and producers for signal bit
// consumers and producers

sealed class GetRecordsAbnormality {
    data object Truncation : GetRecordsAbnormality()
    data object RecordsAfterTombstone : GetRecordsAbnormality()
    data object StandardTombstone : GetRecordsAbnormality()
    data object LowSignalBit : GetRecordsAbnormality()
}


sealed class GetRecordsResult {
    data class Success(val records: ArrayList<PuroRecord>, val offset: Long) : GetRecordsResult()
    data class StandardAbnormality(
        val records: ArrayList<PuroRecord>,
        val offset: Long,
        val abnormality: GetRecordsAbnormality
    ) : GetRecordsResult()

    data class LargeRecordStart(
        val partialReadOffset: Long, // This does not represent finished reads
        val largeRecordFragment: ByteBuffer,
        val targetBytes: Long
    ) : GetRecordsResult()
}

sealed class GetLargeSignalRecordResult(open val byteBuffer: ByteBuffer) {
    data class LargeRecordContinuation(override val byteBuffer: ByteBuffer) : GetLargeSignalRecordResult(byteBuffer)
    data class LargeRecordEnd(override val byteBuffer: ByteBuffer) : GetLargeSignalRecordResult(byteBuffer)
}

sealed class DeserialiseLargeReadResult {
    data object IrrelevantTopic : DeserialiseLargeReadResult()

    data object CrcFailure : DeserialiseLargeReadResult()

    data class Standard(val puroRecord: PuroRecord) : DeserialiseLargeReadResult()

    data class SegmentTombstone(val puroRecord: PuroRecord) : DeserialiseLargeReadResult()
}

sealed class ConsumeState {
    data object Standard : ConsumeState()
    data class Large(
        val largeRecordFragments: ArrayList<ByteBuffer>,
        var targetBytes: Long
    ) : ConsumeState()
}

private data class MultiFragmentVlq(val result: Int, val byteCount: Int, val crc8: Byte, val fragmentIndex: Int)

fun rewindAll(bytes: List<ByteBuffer>) = bytes.forEach { it.rewind() }
fun rewindAll(vararg bytes: ByteBuffer) = bytes.forEach { it.rewind() }
fun getMessageCrc(
    encodedSubrecordLength: ByteBuffer,
    encodedTopicLength: ByteBuffer,
    topic: ByteArray,
    encodedKeyLength: ByteBuffer,
    key: ByteBuffer,
    value: ByteBuffer,
): Byte =
    crc8(encodedSubrecordLength) //TODO get on same page about using buffers here - concerned of `ByteBuffer#array` cost
        .withCrc8(encodedTopicLength)
        .withCrc8(topic)
        .withCrc8(encodedKeyLength)
        .withCrc8(key)
        .withCrc8(value)

fun getRecords(
    readBuffer: ByteBuffer,
    initialBufferOffset: Long,
    finalBufferOffset: Long,
    subscribedTopics: List<ByteArray>,
    isEndOfFetch: Boolean = false,
    logger: Logger = NOPLogger.NOP_LOGGER
): GetRecordsResult {
    val records = ArrayList<PuroRecord>()
    var bufferOffset = initialBufferOffset
    var truncationAbnormality = false // Only matters if end-of-fetch

    readBuffer.position(initialBufferOffset.toInt()) //Saftey issue
    readBuffer.limit(finalBufferOffset.toInt()) //Saftey issue

    while (readBuffer.hasRemaining()) {
        // Compare with getLargeRead
        val expectedCrc = readBuffer.get()
        val lengthData = readBuffer.fromSafeVlq() //The readSaftey doesn't actually do anything

        // +2: is for previous two reads
        if (lengthData != null && (RECORD_CRC_BYTES + lengthData.first + lengthData.second > (readBuffer.remaining() + 2))) {
            if (bufferOffset == 0L) { //It is acceptable to start a read with a large message
                val fragmentBuffer = ByteBuffer.allocate(RECORD_CRC_BYTES + lengthData.second + readBuffer.remaining())
                fragmentBuffer.put(expectedCrc)
                fragmentBuffer.put(lengthData.first.toVlqEncoding().rewind())
                fragmentBuffer.put(readBuffer)
                fragmentBuffer.position(finalBufferOffset.toInt())
                return GetRecordsResult.LargeRecordStart(
                    finalBufferOffset, // Not to be confusing but this should be the same as `readBuffer.capacity()`
                    fragmentBuffer,
                    (RECORD_CRC_BYTES + lengthData.second + lengthData.first).toLong()
                )
            } else { //Otherwise, return what we have at this point; subsequent reads will be by the large read function
                return GetRecordsResult.Success(records, bufferOffset)
            }
        }

        val topicLengthData = readBuffer.fromSafeVlq() //The readSaftey doesn't actually do anything
        val topicMetadata = if (topicLengthData != null) {
            readBuffer.getSafeArraySlice(topicLengthData.first)
        } else null

        //TODO: Subject this to microbenchmarks, not sure if this actually matters
        if (topicMetadata == null || !isRelevantTopic(topicMetadata.first, subscribedTopics, listOf(ControlTopic.BLOCK_START))) {
            if (lengthData != null && (RECORD_CRC_BYTES + lengthData.second + lengthData.first) <= readBuffer.remaining()) {
                bufferOffset += (RECORD_CRC_BYTES + lengthData.second + lengthData.first)
                readBuffer.position(bufferOffset.toInt())
                continue
            }
        } else logger.debug("${expectedCrc}, ${topicMetadata.first.decodeToString()}")
        val keyMetadata = readBuffer.fromSafeVlq()
        val keyData = if (keyMetadata != null) {
            readBuffer.getSafeBufferSlice(keyMetadata.first)
        } else null

        val valueData = if (lengthData != null && topicLengthData != null && keyMetadata != null) {
            readBuffer.getSafeBufferSlice(lengthData.first - topicLengthData.second - topicLengthData.first - keyMetadata.second - keyMetadata.first)
        } else null

        if (topicMetadata?.first?.contentEquals(ControlTopic.BLOCK_START.value) == true && valueData?.first?.array()?.first() == 0.toByte())  {
            return GetRecordsResult.StandardAbnormality(records, initialBufferOffset, GetRecordsAbnormality.LowSignalBit)
        }

        // Note: The else branch isn't advancing the offset because it is possible that this is the next batch
        // TODO: while the reasoning above is sound, but we now have a baked in assumption that the only time
        // TODO: ...we will have bad messages is for the outside of fetches
        if (lengthData != null && topicLengthData != null && topicMetadata != null && keyMetadata != null && keyData != null && valueData != null) {
            val (subrecordLength, encodedSubrecordLengthByteCount, crc1) = lengthData
            val (_, _, crc2) = topicLengthData // _,_ are topic length and bit count
            val (topic, crc3) = topicMetadata
            val (_, _, crc4) = keyMetadata  //_,_ are key length and bit count
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)
            val totalLength = RECORD_CRC_BYTES + encodedSubrecordLengthByteCount + subrecordLength

            if ((expectedCrc == actualCrc) && subscribedTopics.any { it.contentEquals(topic) }) {
                records.add(PuroRecord(topic, key, value))
            } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic)) {
                val abnormality = if (isEndOfFetch || readBuffer.hasRemaining()) {
                    GetRecordsAbnormality.RecordsAfterTombstone
                } else {
                    GetRecordsAbnormality.StandardTombstone
                }
                return GetRecordsResult.StandardAbnormality(records, bufferOffset, abnormality)
            }

            bufferOffset += totalLength
        } else {
            truncationAbnormality = true
        }
    }

    return if (truncationAbnormality) {
        GetRecordsResult.StandardAbnormality(
            records,
            bufferOffset,
            GetRecordsAbnormality.Truncation
        )
    } else {
        GetRecordsResult.Success(records, bufferOffset)
    }
}

// Compare with `ByteBuffer.fromVlq`
private fun multiFragmentFromVlq(largeRecordFragments: List<ByteBuffer>, initialFragmentIndex: Int): MultiFragmentVlq {
    var fragmentIndex = initialFragmentIndex

    while (!largeRecordFragments[fragmentIndex].hasRemaining()) {
        fragmentIndex++
        if (fragmentIndex > largeRecordFragments.size - 1) {
            throw IllegalStateException("Large message consumer integrity issue")
        }
    }
    var currentByte = largeRecordFragments[fragmentIndex].get()
    var crc8 = crc8(currentByte)
    var result = (currentByte and 0x7F).toInt()
    var byteCount = 1

    while ((currentByte and 0x80.toByte()) == 0x80.toByte()) {
        while (!largeRecordFragments[fragmentIndex].hasRemaining()) {
            fragmentIndex++
            if (fragmentIndex > largeRecordFragments.size - 1) {
                throw IllegalStateException("Large message consumer integrity issue")
            }
        }
        currentByte = largeRecordFragments[fragmentIndex].get()
        result += ((currentByte and 0x7F).toInt() shl (byteCount * 7))
        crc8 = crc8.withCrc8(currentByte)
        byteCount++
    }

    return MultiFragmentVlq(result, byteCount, crc8, fragmentIndex)
}

private fun multiFragmentArraySlice(
    largeRecordFragments: ArrayList<ByteBuffer>,
    initialFragmentIndex: Int,
    requiredBytes: Int
): Triple<ByteArray, Byte, Int> {
    val remainingBytes =
        largeRecordFragments.slice(initialFragmentIndex..<largeRecordFragments.size).sumOf { it.remaining() }
    var fragmentIndex = initialFragmentIndex
    if (remainingBytes < requiredBytes) {
        throw IllegalStateException("Large message consumer integrity issue")
    }
    val arraySlice = ByteArray(requiredBytes) //!! Allocation
    var bytesWritten = 0
    var batchWriteLength = 0
    while (bytesWritten < requiredBytes) {
        while (!largeRecordFragments[fragmentIndex].hasRemaining()) {
            fragmentIndex++
            if (fragmentIndex > largeRecordFragments.size - 1) {
                throw IllegalStateException("Large message consumer integrity issue")
            }
        }
        batchWriteLength = requiredBytes.coerceAtMost(largeRecordFragments[fragmentIndex].remaining())
            .coerceAtMost(requiredBytes - bytesWritten)
        largeRecordFragments[fragmentIndex].get(arraySlice, bytesWritten, batchWriteLength)
        bytesWritten += batchWriteLength
    }
    return Triple(arraySlice, crc8(arraySlice), fragmentIndex)
}

// Compare with getSignalBitRecords
fun deserialiseLargeRead(
    consumeState: ConsumeState.Large,
    subscribedTopics: List<ByteArray>
): DeserialiseLargeReadResult {
    val largeRecordFragments = consumeState.largeRecordFragments
    rewindAll(largeRecordFragments)
    var fragmentIndex = 0

    while (!largeRecordFragments[fragmentIndex].hasRemaining()) {
        fragmentIndex++
        if (fragmentIndex > largeRecordFragments.size - 1) {
            throw IllegalStateException("Large message consumer integrity issue")
        }
    }
    val expectedCrc = largeRecordFragments[fragmentIndex].get()
    val lengthData = multiFragmentFromVlq(largeRecordFragments, fragmentIndex).also { fragmentIndex = it.fragmentIndex }
    val topicLengthData =
        multiFragmentFromVlq(largeRecordFragments, fragmentIndex).also { fragmentIndex = it.fragmentIndex }
    val topicMetadata = multiFragmentArraySlice(largeRecordFragments, fragmentIndex, topicLengthData.result).also {
        fragmentIndex = it.third
    }

    if (!isRelevantTopic(topicMetadata.first, subscribedTopics, listOf())) {
        return DeserialiseLargeReadResult.IrrelevantTopic
    }

    val keyMetadata =
        multiFragmentFromVlq(largeRecordFragments, fragmentIndex).also { fragmentIndex = it.fragmentIndex }
    val keyData = multiFragmentArraySlice(largeRecordFragments, fragmentIndex, keyMetadata.result).also {
        fragmentIndex = it.third
    }

    val valueLength =
        lengthData.result - topicLengthData.byteCount - topicLengthData.result - keyMetadata.byteCount - keyMetadata.result
    val valueData = multiFragmentArraySlice(largeRecordFragments, fragmentIndex, valueLength)

    val (subrecordLength, encodedSubrecordLengthByteCount, crc1) = lengthData
    val (_, _, crc2) = topicLengthData // _,_ are topic length and byte count
    val (topic, crc3) = topicMetadata
    val (_, _, crc4) = keyMetadata  //_,_ are key length and bit count
    val (key, crc5) = keyData
    val (value, crc6) = valueData

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)
    val totalLength = RECORD_CRC_BYTES + encodedSubrecordLengthByteCount + subrecordLength

    return if ((expectedCrc == actualCrc) && subscribedTopics.any { it.contentEquals(topic) }) {
        DeserialiseLargeReadResult.Standard(PuroRecord(topic, ByteBuffer.wrap(key), ByteBuffer.wrap(value)))
    } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic)) {
        // In practice you'd have to use comically small read buffers to hit this line - probably so small as to hit
        // exceptions elsewhere
        DeserialiseLargeReadResult.SegmentTombstone(
            PuroRecord(
                topic,
                ByteBuffer.wrap(key),
                ByteBuffer.wrap(value)
            )
        )
    } else {
        DeserialiseLargeReadResult.CrcFailure
    }
}

fun getLargeSignalRecords(
    targetBytes: Long,
    collectedBytes: Long,
    readBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long
): GetLargeSignalRecordResult {
    readBuffer.position(initialOffset.toInt()) //Saftey issue
    readBuffer.limit(finalOffset.toInt()) //Saftey issue

    if (targetBytes < collectedBytes) throw IllegalArgumentException("Has collected more than the target bytes")

    if (finalOffset - initialOffset >= targetBytes - collectedBytes) {
        val fragmentBuffer = ByteBuffer.allocate((targetBytes - collectedBytes).toInt()) // Saftey, also allocation

        fragmentBuffer.put(initialOffset.toInt(), readBuffer, 0, (targetBytes - collectedBytes).toInt())
        fragmentBuffer.position(fragmentBuffer.capacity())
        // Talk: Yikes
        // "This method transfers length bytes into this buffer from the given source buffer, starting at the given
        // offset in the source buffer and the given index in this buffer. The positions of both buffers are unchanged."
        return GetLargeSignalRecordResult.LargeRecordEnd(fragmentBuffer)
    } else {
        val fragmentBuffer = ByteBuffer.allocate((finalOffset - initialOffset).toInt()) // Saftey
        fragmentBuffer.put(readBuffer)
        return GetLargeSignalRecordResult.LargeRecordContinuation(fragmentBuffer)
    }
}

fun buildProtoRecordAtIndex(index: Int, record: PuroRecord, protoRecords: Array<SerialisedPuroRecord?>): Int {
    val (serialisedRecord, recordLength) = record.toSerialised()
    protoRecords[index] = serialisedRecord
    return recordLength
}

// TODO: See `createRecordBuffer` logic in test Utils
fun createBatchedSignalRecordBuffer(puroRecords: List<PuroRecord>, logger: Logger?): ByteBuffer {
    val protoRecords = Array<SerialisedPuroRecord?>(puroRecords.size + 2) { null }

    val blockBodySize =// need to shift over 1 for starting record
        puroRecords.mapIndexed { index, record -> buildProtoRecordAtIndex(index + 1, record, protoRecords) }
            .sum()

    val startBlockRecordSize = buildProtoRecordAtIndex(
        0,
        PuroRecord(ControlTopic.BLOCK_START.value, ByteBuffer.wrap(byteArrayOf()), ByteBuffer.wrap(byteArrayOf(0x00))),
        protoRecords
    )

    val endBlockValue = ByteBuffer.allocate(4)
    endBlockValue.putInt(startBlockRecordSize + blockBodySize)

    val endBlockRecordSize = buildProtoRecordAtIndex(
        puroRecords.size + 1,
        PuroRecord(ControlTopic.BLOCK_END.value, ByteBuffer.wrap(byteArrayOf()), endBlockValue),
        protoRecords
    )

    val batchBuffer = ByteBuffer.allocate(startBlockRecordSize + blockBodySize + endBlockRecordSize)

    protoRecords.forEach { record: SerialisedPuroRecord? ->
        val (messageCrc,
            encodedSubrecordLength,
            encodedTopicLength,
            encodedTopic,
            encodedKeyLength,
            key,
            value) = record!!

        batchBuffer.put(messageCrc).put(encodedSubrecordLength).put(encodedTopicLength).put(encodedTopic)
            .put(encodedKeyLength).put(key).put(value)
    }
    return batchBuffer.rewind()
}

// This function is a little FP-heretical but I have at least partially a good reason for it; even if the `recordLength`
//is rather awkward
fun PuroRecord.toSerialised(): Pair<SerialisedPuroRecord, Int> {
    val (topic, key, value) = this
    rewindAll(key, value)

    // Should have the lengths, CRCs ready to go - should hold the lock for as short a time as possible
    val topicLength = topic.size
    val encodedTopicLength = topicLength.toVlqEncoding()
    val keyLength = key.capacity()
    val encodedKeyLength = keyLength.toVlqEncoding()
    val valueLength = value.capacity()

    val subrecordLength =
        encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
    val encodedSubrecordLength = subrecordLength.toVlqEncoding()

    // crc8 + encodedSubrecordLength + (topicLength + topic + keyLength + key + value)
    val recordLength = RECORD_CRC_BYTES + encodedSubrecordLength.capacity() + subrecordLength

    val messageCrc = getMessageCrc(
        encodedSubrecordLength = encodedSubrecordLength,
        encodedTopicLength = encodedTopicLength,
        topic = topic,
        encodedKeyLength = encodedKeyLength,
        key = key,
        value = value
    )

    rewindAll(encodedSubrecordLength, encodedTopicLength, encodedKeyLength, key, value)
    return SerialisedPuroRecord(
        messageCrc,
        encodedSubrecordLength,
        encodedTopicLength,
        topic,
        encodedKeyLength,
        key,
        value
    ) to recordLength
}

// Dimensions are lockStart * fileSizeOnceLockAcquired
fun <T> withFileLockDimensions(
    channel: FileChannel,
    retryDelay: Duration,
    logger: Logger,
    dimensionConsumer: (Long, Long) -> T
): T {
    val initialFileSize = channel.size()
    var lock: FileLock? = null
    val blockEndOffset = (initialFileSize - BLOCK_END_RECORD_SIZE + 1).coerceAtLeast(0)

    do {
        try {
            lock = channel.tryLock(
                blockEndOffset,
                Long.MAX_VALUE - blockEndOffset,
                true
            )
            if (lock == null) {
                Thread.sleep(retryDelay) // Should eventually give up
            }
        } catch (_: OverlappingFileLockException) {
            logger.warn("Hit OverlappingFileLockException, should only happen when testing mutliple clients in same JVM")
            Thread.sleep(retryDelay)
        }
    } while (lock == null)

    val fileSizeOnceLockAcquired = channel.size()
    // TODO: Make as parameter
    val lockStart = if (fileSizeOnceLockAcquired >= BLOCK_END_RECORD_SIZE) {
        fileSizeOnceLockAcquired - BLOCK_END_RECORD_SIZE + 1
    } else {
        0L
    }

    return dimensionConsumer(lockStart, fileSizeOnceLockAcquired)
}

fun getMaybeSignalRecord(
    channel: FileChannel,
    readBuffer: ByteBuffer,
    lockStart: Long,
    lockEnd: Long
): GetRecordsResult {
    readBuffer.clear()
    channel.read(readBuffer, lockStart)
    val maybeBlockEndRecord = getRecords(
        readBuffer,
        0, //lockStart,
        lockEnd - lockStart,
        listOf(ControlTopic.BLOCK_START.value, ControlTopic.BLOCK_END.value, ControlTopic.SEGMENT_TOMBSTONE.value),
        true
    )
    readBuffer.flip()

    return maybeBlockEndRecord
}