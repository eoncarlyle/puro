import com.iainschmitt.PuroRecord
import com.iainschmitt.createRecordBuffer
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


class ConsumerTest {
    @Test
    fun `Happy Path getRecord`() {
        val expectedTopic = "country-codes"
        val expectedKey = ByteBuffer.wrap("usa2025".toByteArray())
        val expectedValue = ByteBuffer.wrap("""
            {
                "id": 233,
                "name": "United States",
                "code": "US",
                "code3": "USA",
                "numeric": "840",
                "emoji": "🇺🇸",
            }""".trimIndent().toByteArray())

        val buffer = createRecordBuffer(
            PuroRecord(expectedTopic, expectedKey, expectedValue)
        )
        val record = getRecord(buffer)
        assertNotNull(record)

        val (actualTopic, actualKey, actualValue) = record
        assertEquals(expectedTopic, actualTopic)
        assertContentEquals(expectedKey.array(), actualKey.array())
        assertContentEquals(expectedValue.array(), actualValue.array())
    }

    @Test
    fun `Zero Message getRecord`() {
        val expectedTopic = ""
        val expectedKey = ByteBuffer.allocate(0)
        val expectedValue = ByteBuffer.allocate(0)

        val buffer = createRecordBuffer(
            PuroRecord(expectedTopic, expectedKey, expectedValue)
        )

        val record = getRecord(buffer)
        assertNotNull(record)

        val (actualTopic, actualKey, actualValue) = record
        assertEquals(expectedTopic, actualTopic)
        assertContentEquals(expectedKey.array(), actualKey.array())
        assertContentEquals(expectedValue.array(), actualValue.array())
    }
}
