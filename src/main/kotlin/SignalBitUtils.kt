import java.nio.ByteBuffer
import kotlin.experimental.and

// This is for common utilities between consumers and producers for signal bit
// consumers and producers

sealed class GetSignalRecordsAbnormality {
    data object Truncation : GetSignalRecordsAbnormality()
    data object RecordsAfterTombstone : GetSignalRecordsAbnormality()
    data object StandardTombstone : GetSignalRecordsAbnormality()
}


sealed class GetSignalRecordsResult {
    data class Success(val records: ArrayList<PuroRecord>, val offset: Long) : GetSignalRecordsResult()
    data class StandardAbnormality(
        val records: ArrayList<PuroRecord>,
        val offset: Long,
        val abnormality: GetSignalRecordsAbnormality
    ) : GetSignalRecordsResult()

    data class LargeRecordStart(
        val offset: Long,
        val largeRecordFragment: ByteBuffer,
        val expectedCrc: Byte,
        val subrecordLength: Int
    ) : GetSignalRecordsResult()
}

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

fun getSignalBitRecords(
    readBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long,
    subscribedTopics: List<ByteArray>,
    //logger: Logger,
    isEndOfFetch: Boolean = false
): GetSignalRecordsResult {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var truncationAbnormality = false // Only matters if end-of-fetch

    readBuffer.position(initialOffset.toInt()) //Saftey issue
    readBuffer.limit(finalOffset.toInt()) //Saftey issue

    while (readBuffer.hasRemaining()) {
        val expectedCrc = readBuffer.get()
        val lengthData = readBuffer.readSafety()?.fromVlq()
        val topicLengthData = readBuffer.readSafety()?.fromVlq()
        val topicMetadata = if (topicLengthData != null) {
            readBuffer.getSafeArraySlice(topicLengthData.first)
        } else null

        //TODO: Subject this to microbenchmarks, not sure if this actually matters
        if (topicMetadata == null || !isRelevantTopic(topicMetadata.first, subscribedTopics)) {
            if (lengthData != null && (RECORD_CRC_BYTES + lengthData.second + lengthData.first) <= readBuffer.remaining()) {
                offset += (RECORD_CRC_BYTES + lengthData.second + lengthData.first)
                continue
            }
        }
        val keyMetadata = readBuffer.readSafety()?.fromVlq()
        val keyData = if (keyMetadata != null) {
            readBuffer.getSafeBufferSlice(keyMetadata.first)
        } else null

        val valueData = if (lengthData != null && topicLengthData != null && keyMetadata != null) {
            readBuffer.getSafeBufferSlice(lengthData.first - topicLengthData.second - topicLengthData.first - keyMetadata.second - keyMetadata.first)
        } else null

        // Note: The else branch isn't advancing the offset because it is possible that this is the next batch
        // TODO: while the reasoning above is sound, but we now have a baked in assumption that the only time
        // TODO: ...we will have bad messages is for the outside of fetches
        if (lengthData != null && topicLengthData != null && topicMetadata != null && keyMetadata != null && keyData != null && valueData != null) {
            val (subrecordLength, encodedSubrecordLengthBitCount, crc1) = lengthData
            val (_, _, crc2) = topicLengthData // _,_ are topic length and bit count
            val (topic, crc3) = topicMetadata
            val (_, _, crc4) = keyMetadata  //_,_ are key length and bit count
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)
            val totalLength = RECORD_CRC_BYTES + encodedSubrecordLengthBitCount + subrecordLength

            if ((expectedCrc == actualCrc) && subscribedTopics.any { it.contentEquals(topic) }) {
                records.add(PuroRecord(topic, key, value))
            } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic)) {
                val abnormality = if (isEndOfFetch || readBuffer.hasRemaining()) {
                    GetSignalRecordsAbnormality.RecordsAfterTombstone
                } else {
                    GetSignalRecordsAbnormality.StandardTombstone
                }
                return GetSignalRecordsResult.StandardAbnormality(records, offset, abnormality)
            }
            offset += totalLength
        } else if (lengthData != null && (RECORD_CRC_BYTES + lengthData.first + lengthData.second < readBuffer.capacity())) {
            val fragmentBuffer = ByteBuffer.allocate(readBuffer.remaining())
            fragmentBuffer.put(readBuffer)
            return GetSignalRecordsResult.LargeRecordStart(offset, fragmentBuffer, expectedCrc, lengthData.first)
        } else {
            truncationAbnormality = true
        }
    }

    return when {
        isEndOfFetch && truncationAbnormality -> GetSignalRecordsResult.StandardAbnormality(
            records,
            offset,
            GetSignalRecordsAbnormality.Truncation
        )

        truncationAbnormality -> throw IllegalStateException("This should never happen")
        else -> GetSignalRecordsResult.Success(records, offset)
    }
}

fun buildProtoRecordAtIndex(index: Int, record: PuroRecord, protoRecords: Array<SerialisedPuroRecord?>): Int {
    val (serialisedRecord, recordLength) = record.toSerialised()
    protoRecords[index] = serialisedRecord
    return recordLength
}
fun createBatchedSignalRecordBuffer(puroRecords: List<PuroRecord>): ByteBuffer {
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