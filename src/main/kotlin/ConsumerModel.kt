import java.nio.ByteBuffer

sealed class ConsumerStartPoint {
    data object StreamBeginning : ConsumerStartPoint()
    data object Latest : ConsumerStartPoint()
}

sealed class ConsumerError {
    data object FailingCrc : ConsumerError()
    data object NoRemainingBuffer : ConsumerError()
    data object HardTransitionFailure : ConsumerError()
}

sealed class ReadTombstoneStatus {
    data object NotTombstoned : ReadTombstoneStatus()
    data object RecordsAfterTombstone : ReadTombstoneStatus()
    data object StandardTombstone : ReadTombstoneStatus()
}

data class StandardRead(
    val finalConsumerOffset: Long,
    val fetchedRecords: List<PuroRecord>,
    val abnormalOffsetWindow: Pair<Long, Long>?,
    val abnormality: GetRecordsAbnormality?
)
typealias ConsumerResult<R> = Either<ConsumerError, R>

sealed class ReadState {
    data object Standard : ReadState()
    data class Large(
        val largeRecordFragments: ArrayList<ByteBuffer>,
        var targetBytes: Long
    ) : ReadState()
}

sealed class DeserialiseLargeReadResult {
    data object IrrelevantTopic : DeserialiseLargeReadResult()

    data object CrcFailure : DeserialiseLargeReadResult()

    data class Standard(val puroRecord: PuroRecord) : DeserialiseLargeReadResult()

    data class SegmentTombstone(val puroRecord: PuroRecord) : DeserialiseLargeReadResult()
}

sealed class GetLargeSignalRecordResult(open val byteBuffer: ByteBuffer) {
    data class LargeRecordContinuation(override val byteBuffer: ByteBuffer) : GetLargeSignalRecordResult(byteBuffer)
    data class LargeRecordEnd(override val byteBuffer: ByteBuffer) : GetLargeSignalRecordResult(byteBuffer)
}