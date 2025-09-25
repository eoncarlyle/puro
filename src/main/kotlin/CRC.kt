import java.nio.ByteBuffer

// Directly converted from Lammert Bies's Libcrc
val sht75CrcTable = byteArrayOf(
    0, 49, 98, 83, -60, -11, -90, -105, -71, -120, -37, -22, 125, 76, 31, 46,
    67, 114, 33, 16, -121, -74, -27, -44, -6, -53, -104, -87, 62, 15, 92, 109,
    -122, -73, -28, -43, 66, 115, 32, 17, 63, 14, 93, 108, -5, -54, -103, -88,
    -59, -12, -89, -106, 1, 48, 99, 82, 124, 77, 30, 47, -72, -119, -38, -21,
    61, 12, 95, 110, -7, -56, -101, -86, -124, -75, -30, -41, 64, 113, 34, 19,
    126, 79, 28, 45, -70, -117, -40, -23, -57, -14, -91, -108, 3, 50, 97, 80,
    -69, -118, -39, -24, 127, 78, 29, 44, 2, 51, 96, 81, -58, -13, -92, -107,
    -8, -55, -102, -85, 60, 13, 94, 111, 65, 112, 35, 18, -123, -76, -29, -42,
    122, 75, 24, 41, -66, -113, -36, -19, -61, -16, -95, -112, 7, 54, 101, 84,
    57, 8, 91, 106, -3, -52, -97, -82, -128, -79, -34, -45, 68, 117, 38, 23,
    -4, -51, -98, -81, 56, 9, 90, 107, 69, 116, 39, 22, -127, -80, -33, -46,
    -65, -114, -35, -20, 123, 74, 25, 40, 6, 55, 100, 85, -62, -15, -96, -111,
    71, 118, 37, 20, -125, -78, -31, -48, -2, -49, -100, -83, 58, 11, 88, 105,
    4, 53, 102, 87, -64, -15, -94, -109, -67, -116, -33, -18, 121, 72, 27, 42,
    -63, -16, -93, -110, 5, 52, 103, 86, 120, 73, 26, 43, -68, -115, -34, -17,
    -126, -77, -32, -47, 70, 119, 36, 21, 59, 10, 89, 104, -1, -50, -99, -84
)

fun crc8(inputStr: ByteArray): Byte {
    var crc: Byte = 0x00
    for (byte in inputStr) {
        crc = sht75CrcTable[(byte.toInt() xor crc.toInt()) and 0xFF]
    }
    return crc
}

fun crc8(inputStr: ByteBuffer): Byte = crc8(inputStr.array())

fun updateCrc8(crc: Byte, value: Byte): Byte {
    return sht75CrcTable[(value.toInt() xor crc.toInt()) and 0xFF]
}

fun Byte.updateCrc8(value: ByteArray): Byte {
    return updateCrc8(this, crc8(value))
}

// Refactor don't duplicated
fun Byte.updateCrc8(value: ByteBuffer): Byte {
    return updateCrc8(this, crc8(value.array()))
}
