import com.iainschmitt.createRecordBuffer
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertContentEquals

class ClientsTest {
    @Test
    fun `Short message conversion`() {
        val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
        val value = ByteBuffer.wrap(byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()))
        val record = createRecordBuffer("testtopic", key, value)

        assertContentEquals(
            byteArrayOf(
                -37,
                19,
                9,
                116,
                101,
                115,
                116,
                116,
                111,
                112,
                105,
                99,
                4,
                -70,
                -33,
                0,
                13,
                -34,
                -83,
                -66,
                -17
            ), record.array()
        )
    }

    @Test
    fun other() {
        val lines = this::class.java.classLoader.getResource("flags.json")?.openStream()?.use { it.readBytes() }

        val key = ByteBuffer.wrap(byteArrayOf())
        val value = ByteBuffer.wrap(lines)
        val record = createRecordBuffer("flags", key, value)

        //TODO rest of this
    }

}
