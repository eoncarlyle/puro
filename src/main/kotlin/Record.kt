import java.nio.ByteBuffer

const val TOMBSTONE_RECORD_LENGTH = 5

const val RECORD_CRC_BYTES = 1

// 1 byte value, 0 byte key, 1 byte topic
const val BLOCK_START_RECORD_SIZE = 6
// 4 byte value, 0 byte key, 1 byte topic
const val BLOCK_END_RECORD_SIZE = 10

// If you don't rewind, you will have problems!
// The Haskellers have a point here!
val TOMBSTONE_RECORD = byteArrayOf(-82, 3, 1, 0, 0)

enum class ControlTopic(val value: ByteArray) {
    SEGMENT_TOMBSTONE(byteArrayOf(0x00)),
    INVALID_MESSAGE(byteArrayOf(0x01)),
    BLOCK_START(byteArrayOf(0x02)),
    BLOCK_END(byteArrayOf(0x03));

    fun matches(topic: ByteBuffer) = topic.capacity() == 1 && value.contentEquals(topic.array())
}

data class PuroRecord(
    val topic: ByteArray,
    val key: ByteBuffer,
    val value: ByteBuffer,
) {
    constructor(stringTopic: String, key: ByteBuffer, value: ByteBuffer) : this(stringTopic.toByteArray(), key, value)
}

