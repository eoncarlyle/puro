import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.use

const val FILE_EXTENSION = "puro"
const val SEGMENT_PREFIX = "segment"

data class ConsumerSegmentEvent(
    val offset: Long,
    val segmentOrder: Int
)

fun getSegmentName(path: Path) =
    path.fileName.toString().substringAfter(SEGMENT_PREFIX).substringBefore(".$FILE_EXTENSION").toIntOrNull() ?: -1

fun getSegmentOrder(path: Path) =
    path.fileName.toString().substringAfter(SEGMENT_PREFIX).substringBefore(".$FILE_EXTENSION").toIntOrNull() ?: -1

// Note: spurious segment problem, needs to read that a segment tombstone has been placed, this will need to change
fun getActiveSegment(streamDirectory: Path): Path {
    if (!streamDirectory.exists()) {
        Files.createDirectories(streamDirectory)
    }

    val topFoundSegment = Files.list(streamDirectory).use { stream ->
        stream.filter { it.fileName.toString().matches(Regex("""${SEGMENT_PREFIX}\d+\.${FILE_EXTENSION}""")) }.max(
            Comparator { p1, p2 -> getSegmentOrder(p1) - getSegmentOrder(p2) })
    }

    return if (topFoundSegment.isPresent) {
        topFoundSegment.get()
    } else {
        streamDirectory.resolve("${SEGMENT_PREFIX}0.${FILE_EXTENSION}").also { Files.createFile(it) }
    }
}
