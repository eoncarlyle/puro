import java.nio.ByteBuffer

// The existence and usage of this function may mean I am misusing buffers
fun ByteBuffer.defensiveReset(): ByteBuffer = this.flip().limit(this.capacity())
