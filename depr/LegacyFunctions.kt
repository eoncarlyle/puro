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

fun hardTransitionSubrecordLength(subrecordLengthMetaLengthSum: Int): Int {
    //messageSize = 1 + capacity(vlq(subrecordLength)) + subrecordLength
    var subrecordLength = subrecordLengthMetaLengthSum - 1

    while (subrecordLengthMetaLengthSum != RECORD_CRC_BYTES + ceilingDivision(
            Int.SIZE_BITS - subrecordLength.countLeadingZeroBits(),
            7
        ) + subrecordLength
    ) {
        subrecordLength--
    }
    return subrecordLength
}

private fun fetch(
    fileChannel: FileChannel,
    records: ArrayList<PuroRecord>,
    producerOffset: Long
): ConsumerResult<FetchSuccess> {
    if (abnormalOffsetWindow == null) {
        return happyPathFetch(fileChannel, records, producerOffset)
    } else {
        readBuffer.clear()
        fileChannel.read(readBuffer)
        readBuffer.flip()
        // Continuation: clean message divide over a fetch boundary
        val continuationResult = getRecord(readBuffer)

        return continuationResult.fold(
            ifRight = { result ->
                consumerOffset += result.second
                abnormalOffsetWindow = null
                return happyPathFetch(fileChannel, records, producerOffset)
            },
            ifLeft = {
                readBuffer.rewind()
                val bufferOffset = (abnormalOffsetWindow!!.second - abnormalOffsetWindow!!.first).toInt()
                readBuffer.position(bufferOffset + 1)

                getRecord(readBuffer)
                    .fold(
                        ifLeft = {
                            Either.Left<ConsumerError, FetchSuccess>(ConsumerError.HardTransitionFailure)
                        },
                        ifRight = { hardProducerTransitionResult ->
                            consumerOffset += hardProducerTransitionResult.second

                            val originalAbnormalOffsetWindow =
                                Pair(abnormalOffsetWindow!!.first, abnormalOffsetWindow!!.second)

                            val (finalConsumerOffset, fetchedRecords, subsequentAbnormalOffsetWindow, readTombstoneStatus) = standardRead(
                                fileChannel,
                                consumerOffset,
                                producerOffset
                            )
                            records.addAll(fetchedRecords)
                            consumerOffset = finalConsumerOffset
                            abnormalOffsetWindow = subsequentAbnormalOffsetWindow

                            // It is of course possible that there are back-to-back hard transition failures,
                            // and this is reflected in the `originalAbnormalOffsetWindow` vs. `subsequentAbnormalOffsetWindow`
                            val result = FetchSuccess.HardTransition(
                                originalAbnormalOffsetWindow.first,
                                originalAbnormalOffsetWindow.second,
                                readTombstoneStatus != ReadTombstoneStatus.RecordsAfterTombstone
                            )
                            right(result)
                        }
                    )
            }
        )
    }
}

private fun standardRead(
    fileChannel: FileChannel,
    startingReadOffset: Long,
    producerOffset: Long
): StandardRead {
    // When I transitioned to returning values rather than carrying out side effects I needed to decide how to best
    // name the return variables that would be mapped to fields by the caller and I used a `read` as a prefix
    var readOffset = startingReadOffset
    logger.info("Starting read offset $readOffset")

    val readRecords = ArrayList<PuroRecord>()
    var readAbnormalOffsetWindow: Pair<Long, Long>? = null
    var lastAbnormality: GetSignalRecordsAbnormality? = null
    var isLastBatch = false
    var isPossibleLastBatch: Boolean

    while (!isLastBatch) {
        isPossibleLastBatch = (producerOffset - readOffset) <= readBufferSize

        readBuffer.clear()
        if (isPossibleLastBatch) {
            readBuffer.limit(((producerOffset - readOffset) % readBufferSize).toInt()) //Saftey!
        }

        fileChannel.position(readOffset)
        fileChannel.read(readBuffer)
        readBuffer.flip()

        //val (batchRecords, offsetChange, abnormality) = getSignalBitRecords(
        when (consumeState) {
            is ConsumeState.Standard -> {
                val getSignalRecordsResult = getSignalBitRecords(
                    readBuffer,
                    0,
                    if (isPossibleLastBatch) {
                        (producerOffset - readOffset) % readBufferSize
                    } else {
                        readBufferSize.toLong()
                    },
                    subscribedTopics,
                    isEndOfFetch = isPossibleLastBatch,
                    logger
                )

                when (getSignalRecordsResult) {
                    is GetSignalRecordsResult.Success -> {
                        readRecords.addAll(getSignalRecordsResult.records)
                        logger.info("Offset change ${getSignalRecordsResult.offset}")
                        readOffset += getSignalRecordsResult.offset
                    }

                    is GetSignalRecordsResult.StandardAbnormality -> {
                        lastAbnormality = getSignalRecordsResult.abnormality
                        readRecords.addAll(getSignalRecordsResult.records)
                        // Talk as of 7ba7441 lastAbnormality wasn't used where it could have been, possible some subtle bugs here
                        if (isPossibleLastBatch && lastAbnormality == GetSignalRecordsAbnormality.Truncation) {
                            readAbnormalOffsetWindow = consumerOffset to producerOffset
                        } else if (lastAbnormality == GetSignalRecordsAbnormality.RecordsAfterTombstone ||
                            lastAbnormality == GetSignalRecordsAbnormality.StandardTombstone && !isPossibleLastBatch
                        ) {
                            break
                        }
                        logger.info("Standard record offset change ${getSignalRecordsResult.offset}")
                        readOffset += getSignalRecordsResult.offset
                    }

                    is GetSignalRecordsResult.LargeRecordStart -> {
                        logger.info("Consumer state transition to large")
                        consumeState = ConsumeState.Large(
                            arrayListOf(getSignalRecordsResult.largeRecordFragment),
                            getSignalRecordsResult.targetBytes
                        )
                        readOffset += getSignalRecordsResult.partialReadOffset
                    }
                }
            }

            is ConsumeState.Large -> {
                // TODO consider if proving this type via thread saftey actually matters
                val currentConsumeState = consumeState as ConsumeState.Large
                // Check if using position is relevant here
                val getLargeSignalRecordsResult = getLargeSignalRecords(
                    currentConsumeState.targetBytes,
                    currentConsumeState.largeRecordFragments.sumOf { it.position() }
                        .toLong(), //! assumes buffers not rewound
                    readBuffer,
                    0,
                    if (isPossibleLastBatch) {
                        (producerOffset - readOffset) % readBufferSize
                    } else {
                        readBufferSize.toLong()
                    }
                )

                currentConsumeState.largeRecordFragments.add(getLargeSignalRecordsResult.byteBuffer)
                val offsetChange =
                    getLargeSignalRecordsResult.byteBuffer.limit()  //Talk: assumes buffers not rewound, also kinda annoying bug found here
                logger.info("Large record offset change ${offsetChange}")
                readOffset += offsetChange

                lastAbnormality = deserialiseLargeReadWithAbnormalityTracking(
                    getLargeSignalRecordsResult,
                    currentConsumeState,
                    readRecords,
                    lastAbnormality
                )
            }
        }
        isLastBatch = isPossibleLastBatch && readOffset == producerOffset
    }
    logger.info("Final read offset $readOffset")

    return StandardRead(
        readOffset,
        readRecords,
        readAbnormalOffsetWindow,
        when (lastAbnormality) {
            is GetSignalRecordsAbnormality.StandardTombstone -> ReadTombstoneStatus.StandardTombstone
            is GetSignalRecordsAbnormality.RecordsAfterTombstone -> ReadTombstoneStatus.RecordsAfterTombstone
            else -> ReadTombstoneStatus.NotTombstoned
        }
    )
}

private fun deserialiseLargeReadWithAbnormalityTracking(
    getLargeSignalRecordsResult: GetLargeSignalRecordResult,
    currentConsumeState: ConsumeState.Large,
    readRecords: ArrayList<PuroRecord>,
    lastAbnormality: GetSignalRecordsAbnormality?
): GetSignalRecordsAbnormality? {
    var abnormality = lastAbnormality
    if (getLargeSignalRecordsResult is GetLargeSignalRecordResult.LargeRecordEnd) {
        val bytes = currentConsumeState.largeRecordFragments.map { it.array() }.reduce { acc, any -> acc + any }
        //logger.info("[Consumer] multibyte: ${bytes.joinToString { it.toString() }}")
        //logger.info("[Consumer] multibyte message: ${bytes.decodeToString()}")
        when (val deserialisedLargeRead = deserialiseLargeRead(currentConsumeState, subscribedTopics)) {
            is DeserialiseLargeReadResult.Standard -> {
                readRecords.add(deserialisedLargeRead.puroRecord)
            }

            is DeserialiseLargeReadResult.SegmentTombstone -> {
                readRecords.add(deserialisedLargeRead.puroRecord)
                abnormality = GetSignalRecordsAbnormality.StandardTombstone
            }

            is DeserialiseLargeReadResult.IrrelevantTopic -> {
                logger.info("Large message has irrelevant topic")
            }

            is DeserialiseLargeReadResult.CrcFailure -> logger.warn("CRC failure on large message")
        }
        logger.info("Consumer state transition to standard")
        consumeState = ConsumeState.Standard
    }
    return abnormality
}
