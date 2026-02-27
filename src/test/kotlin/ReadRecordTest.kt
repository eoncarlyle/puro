import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ReadRecordTest {

    val records = listOf(
        "AD" to "\uD83C\uDDE9",
        "AE" to "\uD83C\uDDE6\uD83C\uDDEA",
        "AG" to "\uD83C\uDDE6\uD83C\uDDEC",
        "AI" to "\uD83C\uDDEE",
        "AL" to "\uD83C\uDDE6\uD83C\uDDF1",
        "AM" to "\uD83C\uDDE6\uD83C\uDDF2",
        "AO" to "\uD83C\uDDE6\uD83C\uDDF4",
        "AQ" to "\uD83C\uDDE6\uD83C\uDDF6"
    ).map { PuroRecord("flags".toByteArray(), it.first.toByteBuffer(), it.second.toByteBuffer())  }

    @Test
    fun `Basic Operation`() {
        val readRecords = ReadRecords()
        readRecords.add(records[0])
        readRecords.addAll(records.slice(1..<3))

        assertContentEquals(listOf("AD", "AE", "AG"), readRecords.get().map { String(it.key.array()) })

        readRecords.rewind()

        readRecords.addAll(records.slice(3..<5))
        readRecords.add(records[5])
        readRecords.add(records[6])

        assertContentEquals(listOf("AI", "AL", "AM", "AO"), readRecords.get().map { String(it.key.array()) })

        readRecords.rewind()
        readRecords.add(records[7])

        assertEquals("AQ", String(readRecords.get().first().key.array()))
    }
}