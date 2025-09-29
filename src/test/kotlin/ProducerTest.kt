import com.iainschmitt.PuroRecord
import com.iainschmitt.createRecordBuffer
import org.junit.jupiter.api.Test
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

    @Test
    fun other() {
        val lines = this::class.java.classLoader.getResource("flags.json")?.openStream()?.use { it.readBytes() }

        val key = ByteBuffer.wrap(byteArrayOf())
        val value = ByteBuffer.wrap(lines)
        val topicName = """
            b3e5d4756e68638dddebdf06f5d9a479fb138d5a741b8d7ccc4df0bc4a7acc3a
            d22562c6575e16582f2faad1a3c793979065efcdf3a144f648c2da08ebba9b7b
            871a056edf84cffd5ad40ddd0e7b4d137e606418b20755a5ff11b704dc132a27
            64bd5a59d9eba6b6c8bdfb916755326065ce10f6df6f5f11701d8bc470e83a3c
            1321b67afb00280a3aa9dcff98f863e844e95c85c9ff3eb67c5bf2a0bbf19cf4
            74047ff4ace0ada002c4a645f3d58002e7250298c40c1e542b26a4154fb5d8b3
            ee230e73a07fc02637394da96044de4124ac7c509f3da45a53a491a9a0a20226
            ba49a62c0f4b625ad3fd8359abbdfda8f595b92ccba10a7db986ada75515509b
            d432295b714a41282c22987060b676d4cd4845c5547dcbb942de889c3da47d07
            760218082c9de4b20662e817adc89ac881876c4bdb80c7204206658d50f6bb8b
        """.trimIndent()
        val record = createRecordBuffer(PuroRecord(topicName, key, value))

        //TODO rest of this
        getRecord(record)
    }

}
