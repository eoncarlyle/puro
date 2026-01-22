import org.slf4j.Logger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import kotlin.io.path.exists
import kotlin.use

const val FILE_EXTENSION = "puro"
const val SEGMENT_PREFIX = "stream"

data class ConsumerSegmentEvent(
    val offset: Long,
    val segmentOrder: Int
)

fun getSegmentName(path: Path) =
    path.fileName.toString().substringAfter(SEGMENT_PREFIX).substringBefore(".$FILE_EXTENSION").toIntOrNull() ?: -1

fun isSegment(path: Path) = path.fileName.toString().startsWith(SEGMENT_PREFIX)

fun getSegmentOrder(path: Path) =
    path.fileName.toString().substringAfter(SEGMENT_PREFIX).substringBefore(".$FILE_EXTENSION").toIntOrNull() ?: -1

// Note: spurious segment problem, needs to read that a segment tombstone has been placed, this will need to change
fun getActiveSegment(streamDirectory: Path, asProducer: Boolean = false): Path {
    if (!streamDirectory.exists()) {
        throw RuntimeException("Stream directory $streamDirectory does not exist")
    }

    val topFoundSegment = Files.list(streamDirectory).use { paths ->
        paths.filter { it.fileName.toString().matches(Regex("""${SEGMENT_PREFIX}\d+\.${FILE_EXTENSION}""")) }
            .max { p1, p2 ->
                getSegmentOrder(
                    p1
                ) - getSegmentOrder(p2)
            }
    }

    return if (topFoundSegment.isPresent) {
        topFoundSegment.get()
    } else if (asProducer) {
        streamDirectory.resolve("${SEGMENT_PREFIX}0.${FILE_EXTENSION}").also { Files.createFile(it) }
    } else {
        throw RuntimeException("No stream files exist at $streamDirectory")
    }
}

fun getHighestSegmentOrder(streamDirectory: Path, readBuffer: ByteBuffer, retryDelay: Duration, logger: Logger): Int {
    if (!streamDirectory.exists()) {
        throw RuntimeException("Stream directory $streamDirectory does not exist")
    }
    return Files.list(streamDirectory)
        .sorted { a, b -> getSegmentOrder(a) - getSegmentOrder(b) }
        .filter { a -> isSegment(a) }
        .filter { path ->
            return@filter FileChannel.open(path, StandardOpenOption.READ).use { channel ->
                withFileLockDimensions(channel, retryDelay, logger) { lockStart, _ ->
                    return@withFileLockDimensions if (lockStart > 0) {
                        readBuffer.clear()
                        val record = getMaybeSignalRecord(
                            channel,
                            readBuffer,
                            lockStart,
                            lockStart + BLOCK_START_RECORD_SIZE + 1
                        )
                        readBuffer.clear()
                        record is GetSignalRecordsResult.Success && ControlTopic.BLOCK_START.equals(record.records.first().topic)
                    } else {
                        false
                    }
                }
            }
        }
        .map { getSegmentOrder(it) }
        .findFirst().orElse(-1)
}

fun getLowestSegmentOrder(streamDirectory: Path): Int {
    if (!streamDirectory.exists()) {
        throw RuntimeException("Stream directory $streamDirectory does not exist")
    }
    return Files.list(streamDirectory).use { paths -> paths.map { getSegmentOrder(it) }.max { a, b -> a - b } }
        .orElse(-1)
}

fun getSegmentPath(streamDirectory: Path, segmentOrder: Int): Path? {
    return if (Files.exists(streamDirectory) && Files.exists(streamDirectory.resolve("$SEGMENT_PREFIX$segmentOrder.$FILE_EXTENSION"))) {
        streamDirectory.resolve("$SEGMENT_PREFIX$segmentOrder.$FILE_EXTENSION")
    } else null
}
