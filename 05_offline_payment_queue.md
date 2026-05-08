# Assignment 05 — Offline-First Payment Queue

**Difficulty:** 🔴 Senior  
**Focus:** Room Database · WorkManager · Retrofit · Conflict resolution · Idempotency · Exponential back-off

---

## Problem Statement

You're building the payments module for **StreetPay**, a point-of-sale app used by street vendors in areas with unreliable connectivity. Payments must be accepted offline, queued locally, and synced to the server when connectivity returns — with zero duplicate charges and full audit trail.

---

## Real-World Scenario

A food truck vendor at a festival takes 50 payments during a 2-hour connectivity outage. When the network returns, all payments must sync to the payment gateway in order, each exactly once, with idempotency keys to prevent double-charges. Failed payments must retry with back-off. Completed payments must never be re-sent.

---

## Functional Requirements

1. Accept a `Payment` object and persist immediately to Room (offline-safe)
2. Assign a stable `idempotencyKey` (UUID) at creation time
3. `PaymentSyncWorker` (WorkManager) syncs all `PENDING` payments on network availability
4. Each payment transitions: `PENDING → SYNCING → COMPLETED | FAILED | DEAD_LETTER`
5. Retry: max 5 attempts, exponential back-off (2^attempt seconds, capped at 5 min)
6. Dead-letter queue for permanently failed payments (manual review)
7. Observe payment status as a `Flow<List<PaymentWithStatus>>`

---

## Non-Functional Requirements

- Idempotency: re-sending the same `idempotencyKey` must be safe (server returns 200, not 400)
- WorkManager constraint: `NetworkType.CONNECTED`
- Sync is atomic per payment: failure of one does not affect others
- Room migrations handled (schema version bump covered)
- No payment lost on process death (Room write before returning to caller)
- Sync worker is idempotent: running it twice doesn't double-sync

---

## Constraints & Edge Cases

- Server returns `409 Conflict` for already-processed idempotency key → mark `COMPLETED`, not `FAILED`
- Server returns `402 Payment Required` (card declined) → mark `DEAD_LETTER` immediately (no retry)
- Network drops mid-sync → payment stays `SYNCING`, next worker run picks it up and retries
- App killed during sync → WorkManager reschedules automatically
- Two workers running simultaneously → use DB-level optimistic locking (CAS on status field)

---

## Expected Architecture

```
PaymentRepository
    ├── createPayment(Payment)        → Room INSERT → enqueueSync()
    ├── observePayments()             → Flow<List<PaymentEntity>>
    └── enqueueSync()                 → WorkManager.enqueueUniqueWork()

PaymentSyncWorker : CoroutineWorker
    ├── queryPendingPayments()        → Room SELECT WHERE status = PENDING
    ├── for each payment:
    │   ├── markSyncing()             → UPDATE status = SYNCING
    │   ├── api.submitPayment()       → Retrofit POST
    │   ├── markCompleted() or
    │   └── markFailed(attempt++)
    └── retry policy: ExponentialBackOffPolicy

Room Schema:
    payments(id, amount, currency, merchant_id, idempotency_key,
             status, attempt_count, last_error, created_at, synced_at)
```

---

## Sample Input/Output

```kotlin
// Offline: vendor accepts payment
val payment = repo.createPayment(
    amount = 8.50, currency = "USD", merchantId = "m-42"
)
// → Immediately: PaymentEntity(status=PENDING, idempotencyKey="uuid-...")

// Network returns → WorkManager fires
// → POST /payments { idempotencyKey: "uuid-...", amount: 8.50 }
// → Response 200 → PaymentEntity(status=COMPLETED, syncedAt=...)

// Flow observation in UI:
repo.observePayments().collect { payments ->
    val pending   = payments.count { it.status == PaymentStatus.PENDING }
    val completed = payments.count { it.status == PaymentStatus.COMPLETED }
    println("$pending pending, $completed synced")
}
```

---

## Suggested APIs / Classes

```kotlin
@Entity @Dao @Database                          // Room
CoroutineWorker, WorkManager                    // WorkManager
Constraints.Builder().setRequiredNetworkType()  // network constraint
WorkRequest.Builder().setBackoffCriteria(BackoffPolicy.EXPONENTIAL, ...)
OneTimeWorkRequestBuilder<PaymentSyncWorker>()
workManager.enqueueUniqueWork("payment-sync", ExistingWorkPolicy.KEEP, request)
@Transaction fun updateStatusCAS(id, expectedStatus, newStatus): Int  // optimistic lock
Retrofit @POST @Header("Idempotency-Key")
```

---

## Follow-Up Interview Questions

1. What is an idempotency key and why is UUID generated client-side (not server-side)?
2. Why `ExistingWorkPolicy.KEEP` instead of `REPLACE`? When would you use each?
3. How does optimistic locking with a CAS UPDATE prevent double-sync in a race condition?
4. A payment is stuck in `SYNCING` forever (app crash). How do you detect and recover it?
5. How would you implement a "sync now" button that bypasses WorkManager's scheduling?
6. Why is `CoroutineWorker` preferred over `ListenableWorker`?

---

## Common Mistakes

- Generating idempotency key on each retry (defeats the purpose — must be stable)
- Not handling `409 Conflict` as success — causes permanent retry loops
- Using `REPLACE` work policy — cancels in-flight sync and starts over
- Writing payment to Room asynchronously — process death between accept and persist
- Not resetting `SYNCING` payments to `PENDING` on worker start (stuck in SYNCING forever)
- Missing database index on `status` — full table scan on every sync

---

## Optimization Discussion

**Bulk sync:** Instead of one API call per payment, batch up to 100 payments in one `POST /payments/batch`. Dramatically reduces latency on large queues. Server must support batch idempotency.  
**Stuck payment recovery:** On worker start, `UPDATE status='PENDING' WHERE status='SYNCING' AND updated_at < NOW() - 5min`. This recovers payments left syncing by a crashed process.  
**Priority:** Use a `priority` column — vendor-initiated refunds should sync before new charges.  
**Observability:** Emit `WorkInfo` states to analytics. Track `p99` sync latency.

---

## Full Solution

```kotlin
import android.content.Context
import androidx.room.*
import androidx.work.*
import kotlinx.coroutines.flow.Flow
import retrofit2.http.*
import java.util.UUID
import java.util.concurrent.TimeUnit

// ─── Domain Models ───────────────────────────────────────────────────────────

enum class PaymentStatus { PENDING, SYNCING, COMPLETED, FAILED, DEAD_LETTER }

@Entity(
    tableName = "payments",
    indices = [Index("status"), Index("idempotency_key", unique = true)]
)
data class PaymentEntity(
    @PrimaryKey val id: String = UUID.randomUUID().toString(),
    val amount: Double,
    val currency: String,
    val merchantId: String,
    val idempotencyKey: String = UUID.randomUUID().toString(),
    val status: PaymentStatus = PaymentStatus.PENDING,
    val attemptCount: Int = 0,
    val lastError: String? = null,
    val createdAt: Long = System.currentTimeMillis(),
    val syncedAt: Long? = null
)

// ─── DAO ─────────────────────────────────────────────────────────────────────

@Dao
interface PaymentDao {

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insert(payment: PaymentEntity)

    @Query("SELECT * FROM payments WHERE status = 'PENDING' ORDER BY created_at ASC")
    suspend fun getPendingPayments(): List<PaymentEntity>

    @Query("SELECT * FROM payments ORDER BY created_at DESC")
    fun observeAll(): Flow<List<PaymentEntity>>

    /** Atomic CAS update — returns 1 if updated, 0 if status no longer matches (race condition) */
    @Query("""
        UPDATE payments 
        SET status = :newStatus, attempt_count = :attemptCount, last_error = :lastError, 
            synced_at = :syncedAt
        WHERE id = :id AND status = :expectedStatus
    """)
    suspend fun updateStatusCAS(
        id: String,
        expectedStatus: PaymentStatus,
        newStatus: PaymentStatus,
        attemptCount: Int,
        lastError: String?,
        syncedAt: Long?
    ): Int

    /** Reset SYNCING → PENDING for crash recovery (run at worker start) */
    @Query("UPDATE payments SET status = 'PENDING' WHERE status = 'SYNCING'")
    suspend fun resetStuckSyncingPayments()
}

// ─── Database ─────────────────────────────────────────────────────────────────

@Database(entities = [PaymentEntity::class], version = 1, exportSchema = true)
@TypeConverters(PaymentStatusConverter::class)
abstract class PaymentDatabase : RoomDatabase() {
    abstract fun paymentDao(): PaymentDao

    companion object {
        @Volatile private var INSTANCE: PaymentDatabase? = null

        fun getInstance(context: Context): PaymentDatabase =
            INSTANCE ?: synchronized(this) {
                Room.databaseBuilder(context, PaymentDatabase::class.java, "payments.db")
                    .fallbackToDestructiveMigration()
                    .build()
                    .also { INSTANCE = it }
            }
    }
}

class PaymentStatusConverter {
    @TypeConverter fun fromStatus(status: PaymentStatus) = status.name
    @TypeConverter fun toStatus(name: String) = PaymentStatus.valueOf(name)
}

// ─── Retrofit API ─────────────────────────────────────────────────────────────

data class PaymentRequest(
    val amount: Double,
    val currency: String,
    val merchantId: String
)

data class PaymentResponse(val paymentId: String, val status: String)

interface PaymentApi {
    @POST("payments")
    suspend fun submitPayment(
        @Header("Idempotency-Key") idempotencyKey: String,
        @Body request: PaymentRequest
    ): retrofit2.Response<PaymentResponse>
}

// ─── Repository ───────────────────────────────────────────────────────────────

class PaymentRepository(
    private val dao: PaymentDao,
    private val workManager: WorkManager
) {
    fun observePayments(): Flow<List<PaymentEntity>> = dao.observeAll()

    suspend fun createPayment(amount: Double, currency: String, merchantId: String): PaymentEntity {
        val payment = PaymentEntity(amount = amount, currency = currency, merchantId = merchantId)
        dao.insert(payment)        // Synchronous write — survives process death
        enqueueSync()
        return payment
    }

    fun enqueueSync() {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val request = OneTimeWorkRequestBuilder<PaymentSyncWorker>()
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()

        // KEEP: don't cancel in-flight sync if already running
        workManager.enqueueUniqueWork("payment-sync", ExistingWorkPolicy.KEEP, request)
    }
}

// ─── Sync Worker ──────────────────────────────────────────────────────────────

class PaymentSyncWorker(
    appContext: Context,
    params: WorkerParameters
) : CoroutineWorker(appContext, params) {

    private val db  = PaymentDatabase.getInstance(appContext)
    private val dao = db.paymentDao()

    // In production, inject via Hilt WorkerFactory
    private val api: PaymentApi by lazy { buildApi() }

    override suspend fun doWork(): Result {
        // Crash recovery: reset any SYNCING payments from previous crashed run
        dao.resetStuckSyncingPayments()

        val pending = dao.getPendingPayments()
        if (pending.isEmpty()) return Result.success()

        var anyFailure = false

        for (payment in pending) {
            // CAS: mark SYNCING (fails if another worker already grabbed this payment)
            val grabbed = dao.updateStatusCAS(
                id = payment.id,
                expectedStatus = PaymentStatus.PENDING,
                newStatus = PaymentStatus.SYNCING,
                attemptCount = payment.attemptCount,
                lastError = null,
                syncedAt = null
            )
            if (grabbed == 0) continue  // Another worker took it — skip

            val result = syncPayment(payment)
            anyFailure = anyFailure || !result
        }

        return if (anyFailure) Result.retry() else Result.success()
    }

    private suspend fun syncPayment(payment: PaymentEntity): Boolean {
        return try {
            val response = api.submitPayment(
                idempotencyKey = payment.idempotencyKey,
                request = PaymentRequest(payment.amount, payment.currency, payment.merchantId)
            )

            when {
                response.isSuccessful || response.code() == 409 -> {
                    // 409 = already processed — safe to mark complete
                    dao.updateStatusCAS(payment.id, PaymentStatus.SYNCING, PaymentStatus.COMPLETED,
                        payment.attemptCount, null, System.currentTimeMillis())
                    true
                }
                response.code() == 402 -> {
                    // Card declined — no retry
                    dao.updateStatusCAS(payment.id, PaymentStatus.SYNCING, PaymentStatus.DEAD_LETTER,
                        payment.attemptCount + 1, "Card declined (402)", null)
                    true  // Don't trigger WorkManager retry for dead-letter
                }
                else -> {
                    val newAttempt = payment.attemptCount + 1
                    val newStatus = if (newAttempt >= MAX_RETRIES) PaymentStatus.DEAD_LETTER
                                   else PaymentStatus.PENDING
                    dao.updateStatusCAS(payment.id, PaymentStatus.SYNCING, newStatus,
                        newAttempt, "HTTP ${response.code()}", null)
                    newStatus != PaymentStatus.DEAD_LETTER
                }
            }
        } catch (e: Exception) {
            val newAttempt = payment.attemptCount + 1
            val newStatus = if (newAttempt >= MAX_RETRIES) PaymentStatus.DEAD_LETTER
                           else PaymentStatus.PENDING
            dao.updateStatusCAS(payment.id, PaymentStatus.SYNCING, newStatus,
                newAttempt, e.message, null)
            false
        }
    }

    private fun buildApi(): PaymentApi {
        return retrofit2.Retrofit.Builder()
            .baseUrl("https://api.streetpay.io/v1/")
            .addConverterFactory(retrofit2.converter.gson.GsonConverterFactory.create())
            .build()
            .create(PaymentApi::class.java)
    }

    companion object {
        const val MAX_RETRIES = 5
    }
}
```

---

## Alternative Approaches & Tradeoffs

| Approach | Pro | Con |
|---|---|---|
| WorkManager (this solution) | Survives process death, system-managed | Can't control exact timing, min 15min periodic |
| Foreground Service | Real-time sync, immediate | Battery drain, user-visible, killed on swipe |
| Manual retry in ViewModel | Simple | Lost on process death, no network awareness |
| Firebase offline persistence | Zero-code sync | Vendor lock-in, no custom conflict resolution |

---

## Unit Test Examples

```kotlin
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.Assert.*

class PaymentDaoTest {
    private lateinit var db: PaymentDatabase
    private lateinit var dao: PaymentDao

    @Before
    fun setup() {
        db = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(), PaymentDatabase::class.java
        ).allowMainThreadQueries().build()
        dao = db.paymentDao()
    }

    @After
    fun teardown() = db.close()

    @Test
    fun `insert and observe payment`() = runTest {
        val payment = PaymentEntity(amount = 9.99, currency = "USD", merchantId = "m1")
        dao.insert(payment)
        val all = dao.observeAll().first()
        assertEquals(1, all.size)
        assertEquals(PaymentStatus.PENDING, all.first().status)
    }

    @Test
    fun `CAS update succeeds when status matches`() = runTest {
        val payment = PaymentEntity(amount = 5.0, currency = "USD", merchantId = "m1")
        dao.insert(payment)
        val updated = dao.updateStatusCAS(payment.id, PaymentStatus.PENDING,
            PaymentStatus.SYNCING, 0, null, null)
        assertEquals(1, updated)
        assertEquals(PaymentStatus.SYNCING, dao.getPendingPayments().also { /* requery */ }.size.let {
            dao.observeAll().first().first().status
        })
    }

    @Test
    fun `CAS update fails when status has changed`() = runTest {
        val payment = PaymentEntity(amount = 5.0, currency = "USD", merchantId = "m1")
        dao.insert(payment)
        // First worker grabs it
        dao.updateStatusCAS(payment.id, PaymentStatus.PENDING, PaymentStatus.SYNCING, 0, null, null)
        // Second worker tries — should fail (returns 0)
        val result = dao.updateStatusCAS(payment.id, PaymentStatus.PENDING,
            PaymentStatus.SYNCING, 0, null, null)
        assertEquals(0, result)
    }

    @Test
    fun `resetStuckSyncingPayments resets SYNCING to PENDING`() = runTest {
        val payment = PaymentEntity(amount = 5.0, currency = "USD", merchantId = "m1")
        dao.insert(payment)
        dao.updateStatusCAS(payment.id, PaymentStatus.PENDING, PaymentStatus.SYNCING, 0, null, null)
        dao.resetStuckSyncingPayments()
        val all = dao.observeAll().first()
        assertEquals(PaymentStatus.PENDING, all.first().status)
    }
}
```
