import com.iainschmitt.PuroRecord
import java.nio.ByteBuffer

fun getRecord(byteBuffer: ByteBuffer): PuroRecord? {
    val expectedCrc = byteBuffer.get()
    val (encodedTotalLength, _, crc1) = byteBuffer.fromVlq()
    val (topicLength, topicLengthBitCount, crc2) = byteBuffer.fromVlq()
    val (topic, crc3) = byteBuffer.getEncodedString(topicLength)
    val (keyLength, keyLengthBitCount, crc4) = byteBuffer.fromVlq()
    val (key, crc5) = byteBuffer.getSubsequence(keyLength)
    val (value, crc6) = byteBuffer.getSubsequence(encodedTotalLength - topicLengthBitCount - topicLength - keyLengthBitCount - keyLength)

    val actualCrc = updateCrc8List(crc1, crc2, crc3, crc4, crc5, crc6)

    return if (expectedCrc == actualCrc) {
        PuroRecord(topic, key, value)
    } else null
}
