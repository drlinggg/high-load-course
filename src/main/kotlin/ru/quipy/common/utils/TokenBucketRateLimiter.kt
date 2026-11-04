package ru.quipy.common.utils

import java.util.concurrent.TimeUnit

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
): RateLimiter {
    private val windowNanos = timeUnit.toNanos(window)
    private var tokens = bucketMaxCapacity.toDouble()
    private var lastRefill = System.nanoTime()

    @Synchronized
    override fun tick(): Boolean {
        val now = System.nanoTime()
        tokens = minOf(bucketMaxCapacity.toDouble(), tokens + (now - lastRefill) * rate / windowNanos.toDouble())
        lastRefill = now
        if (tokens < 1) return false
        tokens--
        return true
    }
}
