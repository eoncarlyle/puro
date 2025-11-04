import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.use

const val FILE_EXTENSION = "puro"
const val SEGMENT_PREFIX = "stream"

sealed class ConsumerSegmentEvent() {
    data class Standard(val offset: Long, val segmentOrder: Int) : ConsumerSegmentEvent()
    data object Starter : ConsumerSegmentEvent()
}

fun getSegmentName(path: Path) =
    path.fileName.toString().substringAfter(SEGMENT_PREFIX).substringBefore(".$FILE_EXTENSION").toIntOrNull() ?: -1

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

fun getHighestSegmentOrder(streamDirectory: Path): Int {
    if (!streamDirectory.exists()) {
        throw RuntimeException("Stream directory $streamDirectory does not exist")
    }
    return Files.list(streamDirectory).use { paths -> paths.map { getSegmentOrder(it) }.min { a, b -> a - b } }
        .orElse(-1)
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
