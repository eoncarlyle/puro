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

