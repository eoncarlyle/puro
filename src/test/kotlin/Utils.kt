import java.nio.ByteBuffer

// TODO: See `createBatchedSignalRecordBuffer` logic
fun createRecordBuffer(record: PuroRecord): ByteBuffer {
    val (messageCrc,
        encodedSubrecordLength,
        encodedTopicLength,
        encodedTopic,
        encodedKeyLength,
        key,
        value) = record.toSerialised().first

    rewindAll(encodedSubrecordLength, encodedTopicLength, encodedKeyLength, key, value)
    val subrecordResult = encodedSubrecordLength.fromVlq()
    val recordLength = RECORD_CRC_BYTES + subrecordResult.first + subrecordResult.second
    encodedSubrecordLength.rewind() //Talk: very annoying bug without this second rewind

    return ByteBuffer.allocate(recordLength).put(messageCrc).put(encodedSubrecordLength).put(encodedTopicLength)
        .put(encodedTopic)
        .put(encodedKeyLength).put(key).put(value).rewind()
}
