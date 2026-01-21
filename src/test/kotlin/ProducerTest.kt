import kotlin.test.Test
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import kotlin.test.assertContentEquals
import kotlin.io.path.Path
import kotlin.test.assertEquals

class ProducerTest {
    /*
    @Test
    fun `Short message conversion`() {
        val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
        val value = ByteBuffer.wrap(byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte()))
        val record = createRecordBuffer(PuroRecord("testtopic".toByteArray(), key, value))

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
    fun `Round byte buffer batching`() {
        val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
        val puroRecords = (0..<9).map { PuroRecord("testtopic".toByteArray(), key, ByteBuffer.wrap(byteArrayOf(it.toByte()))) }

        val batchedBuffers = ArrayList<ByteBuffer>()
        val producer = LegacyPuroProducer(Path("/tmp"), 3)
        producer.sendBatched(puroRecords)  { buffer -> { _: FileChannel -> batchedBuffers.add(buffer) } }
        assertEquals(3, batchedBuffers.size)
    }

    @Test
    fun `Non-round byte buffer batching`() {
        val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
        val puroRecords = (0..<10).map { PuroRecord("testtopic".toByteArray(), key, ByteBuffer.wrap(byteArrayOf(it.toByte()))) }

        val batchedBuffers = ArrayList<ByteBuffer>()
        val producer = LegacyPuroProducer(Path("/tmp"), 3)
        producer.sendBatched(puroRecords)  { buffer -> { _: FileChannel -> batchedBuffers.add(buffer) } }
        assertEquals(4, batchedBuffers.size)
    }

    @Test
    fun `Tombstone record length`() {
        val tombstoneRecord =  PuroRecord(ControlTopic.SEGMENT_TOMBSTONE.value, byteArrayOf().toByteBuffer(), byteArrayOf().toByteBuffer())
        assertEquals(TOMBSTONE_RECORD_LENGTH, createRecordBuffer(tombstoneRecord).capacity())
        assertContentEquals(TOMBSTONE_RECORD, createRecordBuffer(tombstoneRecord).array())
    }
     */
}
