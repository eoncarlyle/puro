import java.nio.ByteBuffer

enum class ControlTopic(val value: ByteArray) {
    SEGMENT_TOMBSTONE(byteArrayOf(0x00)),
    INVALID_MESSAGE(byteArrayOf(0x01));
    fun matches(topic: ByteBuffer) = topic.capacity() == 1 && value.contentEquals(topic.array())
}

data class PuroRecord(
    val topic: ByteArray,
    val key: ByteBuffer,
    val value: ByteBuffer,
)

