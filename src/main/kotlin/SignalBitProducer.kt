import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

class SignalBitProducer {
    val streamDirectory: Path
    val maximumWriteBatchSize: Int
    val readBufferSize: Int
    private var currentSegmentOrder: Int
    private var offset: Int

    companion object {
        // This should be configurable?
        private val retryDelay = 10.milliseconds.toJavaDuration()
    }

    constructor(
        streamDirectory: Path,
        maximumWriteBatchSize: Int,
        readBufferSize: Int = 8192
    ) {
        this.offset = 0
        this.streamDirectory = streamDirectory
        this.maximumWriteBatchSize = maximumWriteBatchSize
        this.readBufferSize = readBufferSize
        this.currentSegmentOrder = getHighestSegmentOrder(streamDirectory)
    }

    // Producer ne
    fun consumeForIntegrityCheck() {

    }

    fun withProducerLock(block: (FileChannel) -> Unit)  {

        FileChannel.open(getActiveSegment(streamDirectory, true), StandardOpenOption.APPEND).use { channel ->
            val fileSize = channel.size()
            var lock: FileLock?
            do {
                lock = channel.tryLock(fileSize, Long.MAX_VALUE - fileSize, false)
                if (lock == null) {
                    Thread.sleep(retryDelay) // Should eventually give up
                }
            } while (lock == null)
            block(channel)
        }
    }
}