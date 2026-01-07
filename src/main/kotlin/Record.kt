import java.nio.ByteBuffer

const val TOMBSTONE_RECORD_LENGTH = 5

const val RECORD_CRC_BYES = 1

// If you don't rewind, you will have problems!
// The Haskellers have a point here!
val TOMBSTONE_RECORD = byteArrayOf(-82, 3, 1, 0, 0)

enum class ControlTopic(val value: ByteArray) {
    SEGMENT_TOMBSTONE(byteArrayOf(0x00)),
    INVALID_MESSAGE(byteArrayOf(0x01));

    fun matches(topic: ByteBuffer) = topic.capacity() == 1 && value.contentEquals(topic.array())
}

data class PuroRecord(
    val topic: ByteArray,
    val key: ByteBuffer,
    val value: ByteBuffer,
) {
    constructor(stringTopic: String, key: ByteBuffer, value: ByteBuffer) : this(stringTopic.toByteArray(), key, value)
}

