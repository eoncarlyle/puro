class ReadRecords {
    private val backingArray = ArrayList<PuroRecord>()
    private var position = 0

    fun add(record: PuroRecord) {
        if (position <= backingArray.size - 1) {
            backingArray[position++] = record
        } else {
            position++
            backingArray.add(record)
        }
    }

    fun addAll(records: List<PuroRecord>) {
        var insertIndex = 0
        val initialPosition = position
        while (insertIndex + initialPosition <= backingArray.size - 1 && insertIndex <= records.size - 1) {
            backingArray[insertIndex + initialPosition] = records[insertIndex]
            insertIndex++
            position++
        }
        while (insertIndex < records.size) {
            backingArray.add(records[insertIndex++])
            position++
        }
    }

    fun get(): List<PuroRecord> {
        return backingArray.slice(0..<position)
    }
    fun rewind() {
        position = 0
    }
}