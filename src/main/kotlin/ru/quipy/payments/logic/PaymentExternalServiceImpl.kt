package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_ATTEMPTS = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val submissionRate = rateLimitPerSec.coerceAtLeast(1)
    private val parallelRequests = properties.parallelRequests
    private val pendingPayments = AtomicInteger()
    private val processingBudgetMillis = requestAverageProcessingTime.toMillis()
    private val retryAfterMillis = (1_000L + submissionRate - 1) / submissionRate
    private val admissionRateLimiter = TokenBucketRateLimiter(
        submissionRate,
        parallelRequests,
        1,
        TimeUnit.SECONDS
    )

    override fun tryReserve(deadline: Long): Long? {
        val now = now()
        val maxPending = ((deadline - now - processingBudgetMillis) * submissionRate / 1_000).coerceAtLeast(1)
        while (true) {
            val pending = pendingPayments.get()
            if (pending.toLong() >= maxPending) return now + retryAfterMillis
            if (pendingPayments.compareAndSet(pending, pending + 1)) break
        }
        if (admissionRateLimiter.tick()) return null
        pendingPayments.decrementAndGet()
        return now + retryAfterMillis
    }

    override fun releaseReservation() {
        pendingPayments.decrementAndGet()
    }

    private val rateLimiter = LeakingBucketRateLimiter(
        submissionRate.toLong(),
        Duration.ofSeconds(1),
        parallelRequests
    )
    private val window = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, 1)
    }

    private fun performPaymentAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val avgProcessingMs = requestAverageProcessingTime.toMillis() * 2

        if (now() + avgProcessingMs > deadline) {
            logger.warn("[$accountName] Skip before throttle, deadline would be missed. payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before submit")
            }
            return
        }

        val latestSubmissionAt = deadline - avgProcessingMs
        if (!rateLimiter.reserveBlockingUntil(latestSubmissionAt)) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for rate permit")
            }
            return
        }

        var slotAcquired = false
        var submitted = false
        try {
            if (!window.tryAcquire(latestSubmissionAt - now())) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded waiting for concurrent request slot")
                }
                return
            }
            slotAcquired = true

            if (now() + avgProcessingMs > deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before HTTP submit")
                }
                return
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    var retry = false
                    try {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        retry = !body.result &&
                            (body.message == "Temporary error" || response.code == 429 || response.code >= 500) &&
                            canRetry(attempt, deadline)
                        if (!retry) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        }
                    } finally {
                        response.close()
                        window.release()
                    }
                    if (retry) performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, attempt + 1)
                }

                override fun onFailure(call: Call, e: IOException) {
                    val retry = canRetry(attempt, deadline)
                    try {
                        if (!retry) {
                            val reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = reason)
                            }
                        }
                    } finally {
                        window.release()
                    }
                    if (retry) performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, attempt + 1)
                }
            })
            submitted = true
        } finally {
            if (slotAcquired && !submitted) window.release()
        }
    }

    private fun canRetry(attempt: Int, deadline: Long): Boolean =
        attempt < MAX_ATTEMPTS && now() + requestAverageProcessingTime.toMillis() < deadline

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
