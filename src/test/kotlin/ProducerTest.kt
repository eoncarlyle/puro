import org.slf4j.LoggerFactory
import org.slf4j.helpers.NOPLogger
import kotlin.test.Test
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ProducerTest {
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

    /*
    @Test
    fun `Round byte buffer batching`() {
        withTempDir(System.currentTimeMillis().toString()) { puroDirectory ->
            val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
            val puroRecords =
                (0..<9).map { PuroRecord("testtopic".toByteArray(), key, ByteBuffer.wrap(byteArrayOf(it.toByte()))) }

            val batchedBuffers = ArrayList<ByteBuffer>()
            val producer = SignalBitProducer(puroDirectory, 3)
            producer.send(puroRecords)
            assertEquals(3, batchedBuffers.size)
        }
    }

    @Test
    fun `Non-round byte buffer batching`() {
        withTempDir(System.currentTimeMillis().toString()) { puroDirectory ->
            val key = ByteBuffer.wrap(byteArrayOf(0xBA.toByte(), 0xDF.toByte(), 0x00, 0xD))
            val puroRecords =
                (0..<10).map { PuroRecord("testtopic".toByteArray(), key, ByteBuffer.wrap(byteArrayOf(it.toByte()))) }

            val batchedBuffers = ArrayList<ByteBuffer>()
            val producer = SignalBitProducer(puroDirectory, 3)
            producer.send(puroRecords)
            assertEquals(4, batchedBuffers.size)
        }
    }
    */

    @Test
    fun `Tombstone record length`() {
        val tombstoneRecord =
            PuroRecord(ControlTopic.SEGMENT_TOMBSTONE.value, byteArrayOf().toByteBuffer(), byteArrayOf().toByteBuffer())
        assertEquals(TOMBSTONE_RECORD_LENGTH, createRecordBuffer(tombstoneRecord).capacity())
        assertContentEquals(TOMBSTONE_RECORD, createRecordBuffer(tombstoneRecord).array())
    }

    @Test
    fun `Happy path production`() {

        withTempDir(System.currentTimeMillis().toString()) { puroDirectory ->
            val segmentPath = Files.createFile(puroDirectory.resolve("stream0.puro"))

            val producer = Producer(puroDirectory, 10, 100)

            val firstValue = """
            No free man shall be seized or imprisoned, or stripped of his rights or possessions, or outlawed or exiled, or
            deprived of his standing in any way, nor will we proceed with force against him, or send others to do so, except by
            the lawful judgment of his equals or by the law of the land."
            """.trimIndent().replace("\n", "")

            producer.send(listOf(PuroRecord("testTopic", "testKey".toByteBuffer(), firstValue.toByteBuffer())))

            val secondValue = """
            All merchants may enter or leave England unharmed and without fear, and may stay or travel within it, by land 
            or water, for purposes of trade, free from all illegal exactions, in accordance with ancient and lawful customs. 
            This, however, does not apply in time of war to merchants from a country that is at war with us. Any such 
            merchants found in our country at the outbreak of war shall be detained without injury to their persons or 
            property, until we or our chief justice have discovered how our own merchants are being treated in the country 
            at war with us. If our own merchants are safe they shall be safe too. 
            """.trimIndent().replace("\n", "")

            val thirdValue = """
            To no one will we sell, to no one deny or delay right or justice. 
            """.trimIndent().replace("\n", "")

            producer.send(
                listOf(
                    PuroRecord("testTopic", "testKey".toByteBuffer(), secondValue.toByteBuffer()),
                    PuroRecord("testTopic", "testKey".toByteBuffer(), thirdValue.toByteBuffer()),
                    PuroRecord("testTopic", "testKey".toByteBuffer(), "SmallValue".toByteBuffer())
                )
            )

            val completeSegment =
                this::class.java.classLoader.getResource("multiRunCompleteSegment.puro")?.openStream()
                    ?.use { it.readBytes() }!!

            val finalSegment = segmentPath.readBytes()

            assertContentEquals(completeSegment, finalSegment)
        }
    }

    @Test
    fun `Segment cleanup on low signal bit`() {
        withTempDir(System.currentTimeMillis().toString()) { puroDirectory ->
            val segmentPath = Files.createFile(puroDirectory.resolve("stream0.puro"))
            val lowSignalBitSegment =
                this::class.java.classLoader.getResource("incompleteSegmentLowSignalBit.puro")?.openStream()
                    ?.use { it.readBytes() }!!
            segmentPath.writeBytes(lowSignalBitSegment)

            val producer = Producer(puroDirectory, 10, 100)

            val secondValue = """
            All fines that have been given to us unjustly and against the law of the land, and all fines that we have exacted
            unjustly, shall be entirely remitted or the matter decided by a majority judgment of the twenty-five barons referred to
            below in the clause for securing the peace (§61) together with Stephen, archbishop of Canterbury, if he can be present,
            and such others as he wishes to bring with him. If the archbishop cannot be present, proceedings shall continue without
            him, provided that if any of the twenty-five barons has been involved in a similar suit himself, his judgment shall be
            set aside, and someone else chosen and sworn in his place, as a substitute for the single occasion, by the rest of the
            twenty-five.
            """.trimIndent().replace("\n", "")

            val thirdValue = """
            Earls and barons shall be fined only by their equals, and in proportion to the gravity of their offence.
            """.trimIndent().replace("\n", "")

            producer.send(
                listOf(
                    PuroRecord("testTopic", "testKey".toByteBuffer(), secondValue.toByteBuffer()),
                    PuroRecord("testTopic", "testKey".toByteBuffer(), thirdValue.toByteBuffer()),
                    PuroRecord("testTopic", "testKey".toByteBuffer(), "TrueProducerSmallValue".toByteBuffer())
                )
            )

            val completeSegment =
                this::class.java.classLoader.getResource("overwrittenCompleteSegment.puro")?.openStream()
                    ?.use { it.readBytes() }!!

            val finalSegment = segmentPath.readBytes()

            assertContentEquals(completeSegment, finalSegment)
        }
    }

    /*
    The point of this test is to ensure that the integrity check is run (albeit on an almost trivial case). Ideally I'd
    do something to prove that the integrity check was run. I don't want to do this though.
     */
    @Test
    fun thirdProducerRead() {
        withTempDir(System.currentTimeMillis().toString()) { puroDirectory ->
            /* Block start message
        --------------------------------------------------------
        VAL:   -68    8    1    2    0    1    0    0    0   35
        GLB:    0    1    2    3    4    5    6    7    8    9
        REL:    0    1    2    3    4    5    6    7    8    9
        --------------------------------------------------------
         */
            val firstProducer = Producer(puroDirectory, 8192)
            firstProducer.send(
                listOf(PuroRecord("testTopic", "testKey".toByteBuffer(), "First".toByteBuffer()))
            )

            val consumer =
                Consumer(puroDirectory, listOf("testTopic"), logger = NOPLogger.NOP_LOGGER) { record, internalLogger ->
                    internalLogger.info("${String(record.topic)}/${String(record.key.array())}/${String(record.value.array())}")
                }

            val secondProducer = Producer(puroDirectory, 8192)

            consumer.run()
            Thread.sleep(100)
            secondProducer.send(
                listOf(
                    PuroRecord("testTopic", "testKey".toByteBuffer(), "Second".toByteBuffer()),
                )
            )
        }
    }
}
