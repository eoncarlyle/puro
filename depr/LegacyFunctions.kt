private fun reterminateSegment() {
    getConsumedSegmentChannel().use { channel ->
        val fileSize = channel.size()
        var lock: FileLock?
        do {
            lock = channel.tryLock(fileSize - 5, Long.MAX_VALUE - fileSize, false)
            if (lock == null) {
                Thread.sleep(retryDelay)
            }
        } while (lock == null)
        val recordBuffer = ByteBuffer.allocate(5)
        channel.read(recordBuffer)
        getRecord(recordBuffer).onRight { (record, _) ->
            if (record.topic.contentEquals(ControlTopic.SEGMENT_TOMBSTONE.value)) {
                logger.info("Segment reterminated by another consumer")
                return
            }
        }

        channel.position(fileSize)
        channel.write(ByteBuffer.wrap(TOMBSTONE_RECORD))
    }
}

/*
if (logger != null) {
    protoRecords.forEach { record: SerialisedPuroRecord? ->
        logger.info("[Producer] Internal message")
        val printBuffer = ByteBuffer.allocate(startBlockRecordSize + blockBodySize + endBlockRecordSize)
        val (messageCrc,
            encodedSubrecordLength,
            encodedTopicLength,
            encodedTopic,
            encodedKeyLength,
            key,
            value) = record!!

        rewindAll(encodedSubrecordLength, encodedTopicLength, encodedKeyLength, key, value)

        printBuffer.put(messageCrc).put(encodedSubrecordLength).put(encodedTopicLength).put(encodedTopic)
            .put(encodedKeyLength).put(key).put(value)
        printBuffer.array().slice(0..<printBuffer.position()).also { logger.info(it.toString()) }

        logger.info("  CRC + next bit: ${record?.messageCrc}, ${record?.encodedSubrecordLength?.array()?.get(0)}")
        val m = record?.value?.array()
        if (m != null && m.size >= 3) {
            logger.info("  Last 3 bits of value length of internal message ${m.slice(m.size - 3..<m.size)}")
        } else {
            logger.info("  Short message")
        }
    }
}
    */

fun getTopicOnPossiblyTruncatedMessage(recordBuffer: ByteBuffer): ByteArray {
    // TODO saftey
    recordBuffer.position(1)
    recordBuffer.fromVlq() //Discarding the length but advancing buffer
    val (topicLength) = recordBuffer.fromVlq()
    val (topic) = recordBuffer.getArraySlice(topicLength)
    return topic
}

