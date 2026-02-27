class ReadRecords {
    private val backingArray = ArrayList<PuroRecord>()
    private var position = 0

    fun add(record: PuroRecord) {
        if (position <= backingArray.size - 2) {
            backingArray[++position] = record
        } else {
            position++
            backingArray.add(record)
        }
    }

    fun addAll(records: List<PuroRecord>) {
        var insertIndex = 0

        while (insertIndex + records.size <= backingArray.size - 2 && insertIndex <= records.size - 1) {
            backingArray[insertIndex + position] = records[insertIndex]
            insertIndex++
        }
        while (insertIndex <= records.size - 1) {
            backingArray.add(records[insertIndex])
        }
    }

    fun get(): List<PuroRecord> {
        return backingArray.slice(0..<position)
    }
}