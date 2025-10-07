import PuroProducer.Companion.retryDelay
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.use
import kotlin.use

fun extractSegmentName(path: Path) =
    path.fileName.toString().substringAfter("stream").substringBefore(".puro").toIntOrNull() ?: -1

val compareBySegmentName = Comparator<Path> { p1, p2 -> extractSegmentName(p1) - extractSegmentName(p2) }

// Note: spurious segment problem, needs to read that a segment tombstone has been placed, this will need to change
fun getActiveSegment(streamDirectory: Path): Path {
    Files.createDirectories(streamDirectory)

    val topFoundSegment = Files.list(streamDirectory).use { stream ->
        stream.filter { it.fileName.toString().matches(Regex("""stream\d+\.puro""")) }.max(compareBySegmentName)
    }

    return if (topFoundSegment.isPresent) {
        topFoundSegment.get()
    } else {
        streamDirectory.resolve("stream0.puro").also { Files.createFile(it) }
    }
}

fun withLock(fileChannel: FileChannel, block: (FileChannel) -> Unit) {
    // There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
    // the active segment could change.
    // This has been marked on the README as 'Active segment transition race condition handling'
    // Best way may be to read if 'tombstone'
    fileChannel.use { channel ->
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
