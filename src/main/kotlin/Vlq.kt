import java.nio.ByteBuffer
import kotlin.experimental.and

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
    if (bytesReq <= 1) {
        // !! Pool
        return ByteBuffer.allocate(1).put(this.last7Bit())
    }
    val finalBuffer = ByteBuffer.allocate(bytesReq)
    //'Little-endian byte order allows us to support arbitrary lengths more easily' from Kafka source
    for (i in 0..<bytesReq - 1) {
        // 0x80: continuation bit
        finalBuffer.put((0x80 + this.get7Bit(i)).toByte())
    }
    finalBuffer.put(this.get7Bit(bytesReq - 1))
    // The max VLQ size shows that there are limits to how large these VLQ buffers can get: could they be pooled?
    return finalBuffer.rewind()
}

//! ByteBuffer state modification
fun ByteBuffer.fromVlq(): Triple<Int, Int, Byte> {
    var currentByte = this.get()
    var crc8 = crc8(currentByte)
    var result = (currentByte and 0x7F).toInt()
    var bitCount = 1

    while ((currentByte and 0x80.toByte()) == 0x80.toByte()) {
        currentByte = this.get()
        result += ((currentByte and 0x7F).toInt() shl (bitCount * 7))
        crc8 = crc8.withCrc8(currentByte)
        bitCount++
    }

    return Triple(result, bitCount, crc8)
}

fun ByteBuffer.readSafety() = if (this.hasRemaining()) { this } else null

//! ByteBuffer state modification
fun ByteBuffer.getEncodedString(length: Int): Pair<String, Byte> {
    val array = ByteArray(length)
    this.get(array, 0, array.size)
    return String(array) to crc8(array)
}

fun ByteBuffer.getBufferSlice(length: Int): Pair<ByteBuffer, Byte> {
    val array = ByteArray(length)
    this.get(array, 0, array.size)
    // Be wary of this wrap - may be costly
    return ByteBuffer.wrap(array) to crc8(array)
}

fun ByteBuffer.getArraySlice(length: Int): Pair<ByteArray, Byte> {
    val array = ByteArray(length)
    this.get(array, 0, array.size)
    return array to crc8(array)
}
