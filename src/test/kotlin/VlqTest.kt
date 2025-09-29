import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals

class VlqTest {
    private fun equivalent(byteBuffer: ByteBuffer, byteArray: ByteArray) = byteBuffer.array().contentEquals(byteArray)
    @Test
    fun `Saturated 1 bit to VLQ`() {
        val expected = byteArrayOf(0x7F)
        val actual = 127.toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Transition to 2 bit to VLQ`() {
        val expected = byteArrayOf(0x80.toByte(), 0x1.toByte())
        val actual = 128.toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Saturated 2 bit to VLQ`() {
        val expected = byteArrayOf(0xFF.toByte(), 0x7F)
        val actual = 16383.toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Transition to 3 bit to VLQ`() {
        val expected = byteArrayOf(0x80.toByte(), 0x80.toByte(), 0x1)
        val actual = 16384.toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Maximum value to VLQ`() {
        val expected = byteArrayOf(-1, -1, -1, 127)
        val actual = maxVlqInt.toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Overflow value to VLQ`() {
        val expected = byteArrayOf()
        val actual = (maxVlqInt + 1).toVlqEncoding()
        assert(equivalent(actual, expected))
    }

    @Test
    fun `Saturated 1 bit from VLQ`() {
        val expected = 127
        val buffer = ByteBuffer.wrap(byteArrayOf(0x7F))
        val actual = buffer.fromVlq()
        assertEquals(expected, actual.second)

        assertEquals(0, buffer.remaining())
    }

    @Test
    fun `Transition to 2 bit from VLQ`() {
        val expected = 128
        val buffer = ByteBuffer.wrap(byteArrayOf(0x80.toByte(), 0x1.toByte()))
        val actual = buffer.fromVlq()
        assertEquals(expected, actual.second)
        assertEquals(0, buffer.remaining())
    }

    @Test
    fun `Saturated 2 bit from VLQ`() {
        val expected = 16383
        val buffer = ByteBuffer.wrap(byteArrayOf(0xFF.toByte(), 0x7F))
        val actual = buffer.fromVlq()
        assertEquals(expected, actual.second)
        assertEquals(0, buffer.remaining())
    }

    @Test
    fun `Transition to 3 bit from VLQ`() {
        val expected = 16384
        val buffer = ByteBuffer.wrap(byteArrayOf(0x80.toByte(), 0x80.toByte(), 0x1))
        val actual = buffer.fromVlq()
        assertEquals(expected, actual.second)
        assertEquals(0, buffer.remaining())
    }

    @Test
    fun `Maximum value from VLQ`() {
        val expected = maxVlqInt
        val buffer = ByteBuffer.wrap(byteArrayOf(-1, -1, -1, 127))
        val actual = buffer.fromVlq()
        assertEquals(expected, actual.second)
        assertEquals(0, buffer.remaining())
    }
}
