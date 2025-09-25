import java.nio.ByteBuffer

fun ceilingDivision(dividend: Int, divisor: Int): Int = (dividend + divisor - 1) / divisor
fun Int.get7Bit(index: Int): Byte = (this shr (index * 7) and 0x7F).toByte()
fun Int.last7Bit(): Byte = (this and 0x7F).toByte()

// 32 bits * (7/8) = 28 bits
const val maxVlqInt = (1 shl 28) - 1

// https://en.wikipedia.org/wiki/Variable-length_quantity
fun Int.toVlqEncoding(): ByteBuffer {
    if (this > maxVlqInt) {
        // !! Possibly should return a better/different type here
        return ByteBuffer.allocate(0)
    }
    val bitsReq = Int.SIZE_BITS - this.countLeadingZeroBits()
    val bytesReq = ceilingDivision(bitsReq, 7)
    if (bytesReq == 1) {
        ByteBuffer.allocate(1).put(this.last7Bit())
    }
    val finalBuffer = ByteBuffer.allocate(bytesReq)
    //'Little-endian byte order allows us to support arbitrary lengths more easily' from Kafka source
    for (i in 0..<bytesReq - 1) {
        // 0x80: continuation bit
        finalBuffer.put((0x80 + this.get7Bit(i)).toByte())
    }
    finalBuffer.put(this.get7Bit(bytesReq - 1))
    // The max VLQ size shows that there are limits to how large these buffers can get: could they be pooled?
    return finalBuffer
}