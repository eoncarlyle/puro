import java.nio.file.Path

class SignalBitProducer {
    val streamDirectory: Path
    val maximumWriteBatchSize: Int
    private var producedSegmentOrder: Int


    constructor(
        streamDirectory: Path,
        maximumWriteBatchSize: Int,
    ) {
        this.streamDirectory = streamDirectory
        this.maximumWriteBatchSize = maximumWriteBatchSize
        this.producedSegmentOrder = getHighestSegmentOrder(streamDirectory)
    }
}