import org.slf4j.Logger
import java.nio.ByteBuffer
import kotlin.experimental.and

// This is for common utilities between consumers and producers for signal bit
// consumers and producers

sealed class GetSignalRecordAbnormality {
    data object Truncation : GetSignalRecordAbnormality()
    data object LargeMessage : GetSignalRecordAbnormality()
    data object RecordsAfterTombstone : GetSignalRecordAbnormality()
    data object StandardTombstone : GetSignalRecordAbnormality()
    data object LowSignalBit : GetSignalRecordAbnormality()
}

fun getSignalBitRecords(
    readBuffer: ByteBuffer,
    initialOffset: Long,
    finalOffset: Long,
    subscribedTopics: List<ByteArray>,
    logger: Logger,
    isEndOfFetch: Boolean = false
): Triple<List<PuroRecord>, Long, GetSignalRecordAbnormality?> {
    val records = ArrayList<PuroRecord>()
    var offset = initialOffset
    var truncationAbnormality = false // Only matters if end-of-fetch

    readBuffer.position(initialOffset.toInt()) //Saftey issue
    readBuffer.limit(finalOffset.toInt()) //Saftey issue

    while (readBuffer.hasRemaining()) {
        val signalByte = readBuffer.get()
        val signalBit = signalByte and 0x01

        if (signalBit != 0.toByte()) {
            // only advancing offset for 'healthy' records
            return Triple(records, offset, GetSignalRecordAbnormality.LowSignalBit)
        }

        val expectedCrc = readBuffer.get()
        val lengthData = readBuffer.readSafety()?.fromVlq()
        val topicLengthData = readBuffer.readSafety()?.fromVlq()
        val topicMetadata = if (topicLengthData != null) {
            readBuffer.getSafeArraySlice(topicLengthData.first)
        } else null

        //TODO: Subject this to microbenchmarks, not sure if this actually matters
        if (topicMetadata == null || !isRelevantTopic(topicMetadata.first, subscribedTopics)) {
            if (lengthData != null && (RECORD_SIGNAL_BYTES + RECORD_CRC_BYTES + lengthData.second + lengthData.first) <= readBuffer.remaining()) {
                offset += (RECORD_SIGNAL_BYTES + RECORD_CRC_BYTES + lengthData.second + lengthData.first)
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
            val (subrecordLength, encodedTotalLengthBitCount, crc1) = lengthData
            val (_, _, crc2) = topicLengthData // _,_ are topic length and bit count
            val (topic, crc3) = topicMetadata
            val (_, _, crc4) = keyMetadata  //_,_ are key length and bit count
            val (key, crc5) = keyData
            val (value, crc6) = valueData

            val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)
            // Equivalent to:
            //val subrecordLength = topicLengthBitCount + topicLength + keyLengthBitCount + keyLength + value.capacity()
            val totalLength = RECORD_SIGNAL_BYTES + RECORD_CRC_BYTES + encodedTotalLengthBitCount + subrecordLength

            if ((expectedCrc == actualCrc) && subscribedTopics.any { it.contentEquals(topic) }) {
                records.add(PuroRecord(topic, key, value))
            } else if (ControlTopic.SEGMENT_TOMBSTONE.value.contentEquals(topic)) {
                val abnormality = if (isEndOfFetch || readBuffer.hasRemaining()) {
                    GetSignalRecordAbnormality.RecordsAfterTombstone
                } else {
                    GetSignalRecordAbnormality.StandardTombstone
                }
                return Triple(records, offset, abnormality)
            }
            // These are necessarily 'interior' messages.
            offset += totalLength
        } else if (lengthData != null && ( RECORD_SIGNAL_BYTES + RECORD_CRC_BYTES + lengthData.first + lengthData.second < readBuffer.capacity())) {
            //TODO: need to return a special return value that includes the existing bytes, should be similar to what I'd do with discriminated unions
        } else {
            truncationAbnormality = true
        }
    }

    return Triple(
        records,
        offset,
        if (isEndOfFetch && truncationAbnormality) GetSignalRecordAbnormality.LargeMessage else null
    )
}