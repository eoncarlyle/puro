import java.nio.ByteBuffer

data class SerialisedPuroRecord(
    val messageCrc: Byte,
    val encodedSubrecordLength: ByteBuffer,
    val encodedTopicLength: ByteBuffer,
    val encodedTopic: ByteArray,
    val encodedKeyLength: ByteBuffer,
    val key: ByteBuffer,
    val value: ByteBuffer
)

enum class ProducerSegmentState {
    INIT, READY, CLEANUP
}