import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


class ConsumerTest {
    @Test
    fun `Happy Path getRecord`() {
        val expectedTopic = "country-codes".toByteArray()
        val expectedKey = "usa2025".toByteBuffer()
        val expectedValue = ByteBuffer.wrap(
            """
            {
                "id": 233,
                "name": "United States",
                "code": "US",
                "code3": "USA",
                "numeric": "840",
                "emoji": "🇺🇸",
            }""".trimIndent().toByteArray()
        )

        val buffer = createRecordBuffer(
            PuroRecord(expectedTopic, expectedKey, expectedValue)
        )

        val result = getRecord(buffer)
        assertTrue { result.isRight() }

        result.map { result ->
            val (actualTopic, actualKey, actualValue) = result.first
            assertContentEquals(expectedTopic, actualTopic)
            assertContentEquals(expectedKey.array(), actualKey.array())
            assertContentEquals(expectedValue.array(), actualValue.array())
        }
    }

    @Test
    fun `Zero Message getRecord`() {
        val expectedTopic = "".toByteArray()
        val expectedKey = ByteBuffer.allocate(0)
        val expectedValue = ByteBuffer.allocate(0)

        val buffer = createRecordBuffer(
            PuroRecord(expectedTopic, expectedKey, expectedValue)
        )

        val result = getRecord(buffer)
        assertTrue { result.isRight() }

        result.map { result ->
            val (actualTopic, actualKey, actualValue) = result.first
            assertContentEquals(expectedTopic, actualTopic)
            assertContentEquals(expectedKey.array(), actualKey.array())
            assertContentEquals(expectedValue.array(), actualValue.array())
        }
    }

    @Test
    fun `Large Message getRecord`() {
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
        """.trimIndent().toByteArray()
        val record = createRecordBuffer(PuroRecord(topicName, key, value))


        // This only works because `getRecord` is null if cheksums
        // don't match, if/when result types are used this need
        assertNotNull(getRecord(record))
    }

    @Test
    fun `Simple getMessages`() {
        val recordBuffers = (0..<4).map {
            createRecordBuffer(
                PuroRecord(
                    "testTopic".toByteArray(), ByteBuffer.wrap(it.toString().toByteArray()),
                    ByteBuffer.wrap(it.toString().hashCode().toString().toByteArray())
                )
            )
        }
        val consumerBuffer = ByteBuffer.allocate(recordBuffers.sumOf { it.remaining() })
        recordBuffers.forEach { record -> consumerBuffer.put(record) }
        consumerBuffer.rewind()

        //TODO Uncomment once logger is removed again
        val getRecordsResult = getSignalBitRecords(
            consumerBuffer,
            0,
            consumerBuffer.capacity().toLong(),
            listOf("testTopic".toByteArray()),
            true
        )

        assertTrue { getRecordsResult is GetSignalRecordsResult.Success }

        when (getRecordsResult) {
            is GetSignalRecordsResult.Success -> {
                assertEquals(4, getRecordsResult.records.size)
            }
            else -> throw AssertionError("Incorrect record type")
        }
    }
}
