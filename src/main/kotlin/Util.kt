import java.nio.ByteBuffer

fun rewindAll(vararg bytes: ByteBuffer) = bytes.forEach { it.rewind() }
