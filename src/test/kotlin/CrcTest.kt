import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals

class CrcTest {

    @Test
    fun `First Random byte conversion`() {
        val bytes = byteArrayOf(
            0x4A, 0x1F, 0x65, 0x2C, 0x41, 0x6E, 0x0A, 0x78, 0x29, 0x54, 0x13, 0x7D, 0x35, 0x69, 0x04, 0x77
        )
        assertEquals(22, crc8(bytes))
        assertEquals(22, crc8(ByteBuffer.wrap(bytes)))
    }

    @Test
    fun `Second Random byte conversion`() {
        val bytes = byteArrayOf(
            0x07, 0x48, 0x74, 0x10, 0x41, 0x6b, 0x70, 0x6c, 0x56, 0x21, 0x0a, 0x0d, 0x1f, 0x13, 0x2e, 0x44
        )
        assertEquals(-71, crc8(bytes))
        assertEquals(-71, crc8(ByteBuffer.wrap(bytes)))
    }
}
