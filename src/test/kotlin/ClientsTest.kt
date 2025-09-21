import com.iainschmitt.toVlq
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class ClientsTest {

    private fun equivalent(byteBuffer: ByteBuffer, byteArray: ByteArray) = byteBuffer.array().contentEquals(byteArray)

    @Test
    fun `Saturated 1 bit VLQ`() {
        val expected = byteArrayOf(0x7F)
        val actual = 127L.toVlq()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Transition to 2 bit VLQ`() {
        val expected = byteArrayOf(0x80.toByte(), 0x1.toByte())
        val actual = (128L).toVlq()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Saturated 2 bit VLQ`() {
        val expected = byteArrayOf(0xFF.toByte(), 0x7F)
        val actual = 16383L.toVlq()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Transition to 3 bit VLQ`() {
        val expected = byteArrayOf(0x80.toByte(), 0x80.toByte(), 0x1)
        val actual = 16384L.toVlq()
        assert(equivalent(actual, expected))
    }

}