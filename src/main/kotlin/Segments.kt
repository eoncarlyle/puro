import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.use

fun extractSegmentName(path: Path) =
    path.fileName.toString().substringAfter("stream").substringBefore(".puro").toIntOrNull() ?: -1

val compareBySegmentName = Comparator<Path> { p1, p2 -> extractSegmentName(p1) - extractSegmentName(p2) }

// Note: spurious segment problem, needs to read that a segment tombstone has been placed, this will need to change
fun getActiveSegmentPath(streamDirectory: Path): Path {
    if (!streamDirectory.exists()) {
        Files.createDirectories(streamDirectory)
    }

    val topFoundSegment = Files.list(streamDirectory).use { stream ->
        stream.filter { it.fileName.toString().matches(Regex("""stream\d+\.puro""")) }.max(compareBySegmentName)
    }

    return if (topFoundSegment.isPresent) {
        topFoundSegment.get()
    } else {
        streamDirectory.resolve("stream0.puro").also { Files.createFile(it) }
    }
}
