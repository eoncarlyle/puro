import java.nio.ByteBuffer

sealed class GetRecordsAbnormality {
    data object Truncation : GetRecordsAbnormality()
    data object RecordsAfterTombstone : GetRecordsAbnormality()
    data object StandardTombstone : GetRecordsAbnormality()
    data object LowSignalBit : GetRecordsAbnormality()
}

sealed class GetRecordsResult {
    data class Success(val records: ArrayList<PuroRecord>, val offset: Long) : GetRecordsResult()
    data class StandardAbnormality(
        val records: ArrayList<PuroRecord>,
        val offset: Long,
        val abnormality: GetRecordsAbnormality
    ) : GetRecordsResult()

    data class LargeRecordStart(
        val partialReadOffset: Long, // This does not represent finished reads
        val largeRecordFragment: ByteBuffer,
        val targetBytes: Long
    ) : GetRecordsResult()
}

data class MultiFragmentVlq(val result: Int, val byteCount: Int, val crc8: Byte, val fragmentIndex: Int)