import com.iainschmitt.PuroRecord
import com.iainschmitt.createRecordBuffer
import kotlin.test.Test
import java.nio.ByteBuffer
import kotlin.test.assertContentEquals

class ProducerTest {
    @Test
    fun `Short message conversion`() {
        val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
        val value = ByteBuffer.wrap(byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()))
        val record = createRecordBuffer(PuroRecord("testtopic", key, value))

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
}
