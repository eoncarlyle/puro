package com.iainschmitt.puro

import PuroRecord
import toByteBuffer
import java.nio.ByteBuffer
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.random.Random

// Adapted from https://github.com/darioradecic/python-1-billion-row-challenge/blob/main/data/createMeasurements.py
class MeasurementFactory(private val stdDev: Double = 10.0) {
    private val rng = Random(1689)

    private fun nextGaussian(mean: Double, std: Double): Double {
        var u: Double
        var v: Double
        var s: Double
        do {
            u = rng.nextDouble() * 2.0 - 1.0
            v = rng.nextDouble() * 2.0 - 1.0
            s = u * u + v * v
        } while (s >= 1.0 || s == 0.0)
        val mul = sqrt(-2.0 * ln(s) / s)
        return mean + std * u * mul
    }

    fun getMeasurements(
        records: Long = 1_000_000L,
    ): ArrayList<PuroRecord> {
        val batchSize = 10_000_000L
        val batches = max(records / batchSize, 1)
        val list = ArrayList<PuroRecord>()

        for (batch in 0 until batches) {
            val from = batch * batchSize
            val to = minOf(from + batchSize, records)
            val count = (to - from).toInt()

            repeat(count) {
                val (name, mean) = stations[rng.nextInt(stations.size)]
                val temp = nextGaussian(mean, stdDev)
                list.add(
                    PuroRecord(
                        "irrelevant topic",
                        rng.nextInt().let { "%016x".format(it) }.toByteBuffer(),
                        ByteBuffer.allocate(4).putInt(rng.nextInt())
                    )
                )
                list.add(PuroRecord("temperatures", name.toByteBuffer(), ByteBuffer.allocate(8).putDouble(temp)))
            }
        }
        return list
    }
}