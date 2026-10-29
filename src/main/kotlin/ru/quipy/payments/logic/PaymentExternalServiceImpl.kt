package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
        val json = "application/json".toMediaType()
    }

    private data class PendingPayment(
        val paymentId: UUID,
        val transactionId: UUID,
        val amount: Int,
        val deadline: Long,
    )

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val window = OngoingWindow(parallelRequests)

    private val pendingPayments = LinkedBlockingQueue<PendingPayment>()

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .readTimeout(Duration.ofMinutes(1))
        .build()

    init {
        meterRegistry.gauge(
            "payment_batch_queue_size",
            listOf(Tag.of("account", accountName)),
            pendingPayments,
        ) { it.size.toDouble() }
        Executors.newSingleThreadExecutor(NamedThreadFactory("payment-batch-$accountName"))
            .submit { processQueue() }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        pendingPayments.put(PendingPayment(paymentId, transactionId, amount, deadline))
    }

    private fun processQueue() {
        while (!Thread.currentThread().isInterrupted) {
            val first = pendingPayments.take()
            val latestSubmissionAt = first.deadline - requestAverageProcessingTime.toMillis()
            val ratePermit = rateLimiter.reserveBlockingUntil(latestSubmissionAt)
            if (ratePermit == null) {
                fail(first, "Deadline exceeded waiting for rate permit")
                continue
            }

            var slotAcquired = false
            var submitted = false
            try {
                if (!window.tryAcquire(latestSubmissionAt - now())) {
                    fail(first, "Deadline exceeded waiting for concurrent request slot")
                    continue
                }
                slotAcquired = true

                val batch = mutableListOf(first)
                pendingPayments.drainTo(batch)
                val validBatch = batch.filter {
                    val valid = now() + requestAverageProcessingTime.toMillis() <= it.deadline
                    if (!valid) fail(it, "Deadline exceeded before HTTP submit")
                    valid
                }
                if (validBatch.isEmpty()) continue

                val timeout = Duration.ofMillis((validBatch.minOf { it.deadline } - now()).coerceAtLeast(1))
                val body = ExternalSysBulkRequest(
                    serviceName,
                    accountName,
                    validBatch.map {
                        ExternalSysPaymentRequest(it.transactionId.toString(), it.paymentId.toString(), it.amount)
                    },
                )
                val request = Request.Builder()
                    .url("http://$paymentProviderHostPort/external/process/bulk?token=$token&timeout=$timeout")
                    .put(mapper.writeValueAsBytes(body).toRequestBody(json))
                    .build()

                ratePermit.markSubmitted()
                client.newCall(request).enqueue(batchCallback(validBatch))
                submitted = true
            } finally {
                ratePermit.close()
                if (slotAcquired && !submitted) window.release()
            }
        }
    }

    private fun batchCallback(batch: List<PendingPayment>) = object : Callback {
        override fun onResponse(call: Call, response: Response) {
            val results = try {
                mapper.readValue(response.body?.string(), ExternalSysBulkResponse::class.java).responses
                    .associateBy { it.transactionId }
            } catch (e: Exception) {
                logger.error("[$accountName] Could not read bulk payment response, result code: ${response.code}", e)
                emptyMap()
            } finally {
                response.close()
                window.release()
            }

            batch.forEach { payment ->
                val result = results[payment.transactionId.toString()]
                paymentESService.update(payment.paymentId) {
                    it.logProcessing(
                        result?.result == true,
                        now(),
                        payment.transactionId,
                        reason = if (result == null) "Missing bulk response" else result.message,
                    )
                }
            }
        }

        override fun onFailure(call: Call, e: IOException) {
            window.release()
            val reason = if (e is SocketTimeoutException) "Request timeout." else e.message
            logger.error("[$accountName] Bulk payment request failed", e)
            batch.forEach { fail(it, reason) }
        }
    }

    private fun fail(payment: PendingPayment, reason: String?) {
        paymentESService.update(payment.paymentId) {
            it.logProcessing(false, now(), payment.transactionId, reason = reason)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
