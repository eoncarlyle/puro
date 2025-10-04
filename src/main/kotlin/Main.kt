import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryWatcher
import java.nio.file.Path
import kotlin.io.path.Path


class DirectoryWatchingUtility(directoryToWatch: Path?) {
    val watcher: DirectoryWatcher? = DirectoryWatcher.builder()
        .path(directoryToWatch) // or use paths(directoriesToWatch)
        .listener { event: DirectoryChangeEvent? ->
            when (event!!.eventType()) {
                DirectoryChangeEvent.EventType.MODIFY -> println(event)
                else -> println(event)
            }
        } // .fileHashing(false) // defaults to true
        // .logger(logger) // defaults to LoggerFactory.getLogger(DirectoryWatcher.class)
        // .watchService(watchService) // defaults based on OS to either JVM WatchService or the JNA macOS WatchService
        .build()

    fun stopWatching() {
        watcher?.close()
    }
}

fun main() {
    println("here")
    DirectoryWatchingUtility(Path("/tmp/puro")).watcher?.watch()
}
