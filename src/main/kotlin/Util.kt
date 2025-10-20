import java.nio.ByteBuffer

fun String.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(this.toByteArray())

fun ByteArray.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(this)
