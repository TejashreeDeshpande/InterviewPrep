# Assignment 08 — AI Assistant Message Streaming

**Difficulty:** 🔴 Senior  
**Focus:** SSE streaming · Kotlin DSL · Flow cancellation · Token throttling · callbackFlow

---

## Problem Statement

You're building the chat interface for **Aria**, an AI-powered mobile assistant. Users expect to see the AI's response appear word-by-word in real time (like ChatGPT). The streaming pipeline must handle cancellation cleanly, throttle token emissions so the UI doesn't flood, and expose a type-safe Kotlin DSL for building prompts.

---

## Functional Requirements

1. `AriaChatClient.stream(prompt: PromptBuilder.() -> Unit): Flow<StreamEvent>`
2. Kotlin DSL: `prompt { system("You are Aria"); user("Explain coroutines") }`
3. SSE parsing: `data: {"token": "hello"}` → `StreamEvent.Token("hello")`
4. `StreamEvent`: `Token(text)`, `Done`, `Error(message)`, `ThinkingIndicator`
5. Token flow throttled to max 20 tokens/sec for natural feel
6. Cancellation: user taps "Stop" → coroutine cancel → server abort via DELETE request

---

## Non-Functional Requirements

- Zero memory leaks: `callbackFlow` with `awaitClose { }` cleanup
- `ThinkingIndicator` emitted if no token arrives within 500ms
- Prompt DSL validated at compile time (no invalid message orders)
- Response concatenated into `StateFlow<String>` for display
- Support multiple concurrent conversations (keyed by `conversationId`)

---

## Constraints & Edge Cases

- SSE connection drops mid-stream → emit `StreamEvent.Error`, allow retry
- User sends message while previous stream is active → cancel previous, start new
- Prompt exceeds token budget → throw `PromptTooLongException` before network call
- Emoji/Unicode tokens arrive split across SSE chunks → buffer until valid UTF-8

---

## Expected Architecture

```
PromptBuilder (DSL)
    └── build(): PromptRequest
            │
            ▼
AriaChatClient
    ├── stream(): Flow<StreamEvent>   ← callbackFlow wrapping OkHttp SSE
    ├── throttle(20 tokens/sec)       ← custom Flow operator
    └── thinkingIndicator()           ← timeout-based side emission

ChatViewModel
    ├── activeStreamJob: Job?         ← cancel on new message
    ├── responseText: StateFlow<String>
    └── send(prompt) → cancels old, starts new stream
```

---

## Sample Input/Output

```kotlin
// DSL usage
client.stream {
    system("You are a helpful coding assistant.")
    user("Explain Kotlin Flow in 3 sentences.")
    temperature(0.7f)
    maxTokens(200)
}.collect { event ->
    when (event) {
        is StreamEvent.Token     -> print(event.text)
        is StreamEvent.Done      -> println("\n[Complete]")
        is StreamEvent.Error     -> println("\n[Error]: ${event.message}")
        is StreamEvent.Thinking  -> println("[Aria is thinking...]")
    }
}
```

---

## Follow-Up Interview Questions

1. Why `callbackFlow` instead of `flow {}` for wrapping callback-based SSE APIs?
2. How does cancelling a coroutine propagate to the OkHttp SSE connection?
3. How would you implement a custom `throttle` operator using `Flow` without `conflate()`?
4. What's the difference between `buffer()`, `conflate()`, and `debounce()` in Flow?
5. How do you ensure the "Stop" button's DELETE request fires even if the UI is closing?
6. How would you add streaming to a REST API that doesn't support SSE (chunked transfer encoding)?

---

## Full Solution

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import okhttp3.*
import okhttp3.sse.*
import java.time.Duration

// ─── DSL ─────────────────────────────────────────────────────────────────────

enum class MessageRole { SYSTEM, USER, ASSISTANT }
data class Message(val role: MessageRole, val content: String)

data class PromptRequest(
    val messages: List<Message>,
    val temperature: Float = 0.7f,
    val maxTokens: Int = 1024
) {
    val estimatedTokens: Int get() = messages.sumOf { it.content.length / 4 }
}

class PromptBuilder {
    private val messages = mutableListOf<Message>()
    var temperature: Float = 0.7f
    var maxTokens: Int = 1024

    fun system(content: String)    { messages.add(Message(MessageRole.SYSTEM, content)) }
    fun user(content: String)      { messages.add(Message(MessageRole.USER, content)) }
    fun assistant(content: String) { messages.add(Message(MessageRole.ASSISTANT, content)) }
    fun temperature(t: Float)      { temperature = t }
    fun maxTokens(n: Int)          { maxTokens = n }

    fun build(): PromptRequest {
        require(messages.isNotEmpty()) { "Prompt must have at least one message" }
        require(messages.last().role == MessageRole.USER) { "Last message must be from user" }
        return PromptRequest(messages.toList(), temperature, maxTokens)
    }
}

// ─── Stream Events ────────────────────────────────────────────────────────────

sealed class StreamEvent {
    data class Token(val text: String) : StreamEvent()
    object Done : StreamEvent()
    object Thinking : StreamEvent()
    data class Error(val message: String) : StreamEvent()
}

// ─── Custom Flow Operators ────────────────────────────────────────────────────

/** Throttle: emit at most [maxPerSecond] items per second */
fun <T> Flow<T>.throttle(maxPerSecond: Int): Flow<T> = flow {
    val intervalMs = 1000L / maxPerSecond
    var lastEmitTime = 0L
    collect { value ->
        val now = System.currentTimeMillis()
        val elapsed = now - lastEmitTime
        if (elapsed < intervalMs) delay(intervalMs - elapsed)
        emit(value)
        lastEmitTime = System.currentTimeMillis()
    }
}

/** Emits ThinkingIndicator if no item arrives within [timeoutMs] */
fun Flow<StreamEvent>.withThinkingIndicator(timeoutMs: Long = 500): Flow<StreamEvent> = flow {
    var thinking = false
    collect { event ->
        thinking = false
        emit(event)
    }
}.let { upstream ->
    channelFlow {
        val timeoutJob = launch {
            delay(timeoutMs)
            send(StreamEvent.Thinking)
        }
        upstream.collect { event ->
            timeoutJob.cancel()
            send(event)
        }
    }
}

// ─── Client ───────────────────────────────────────────────────────────────────

class AriaChatClient(
    private val baseUrl: String = "https://api.aria.ai/v1",
    private val apiKey: String
) {
    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofSeconds(60))
        .build()

    fun stream(block: PromptBuilder.() -> Unit): Flow<StreamEvent> {
        val prompt = PromptBuilder().apply(block).build()

        if (prompt.estimatedTokens > 4096) {
            return flow { emit(StreamEvent.Error("Prompt exceeds token budget")) }
        }

        return callbackFlow {
            val request = Request.Builder()
                .url("$baseUrl/chat/stream")
                .header("Authorization", "Bearer $apiKey")
                .header("Accept", "text/event-stream")
                .post(buildBody(prompt))
                .build()

            val factory = EventSources.createFactory(client)
            val source = factory.newEventSource(request, object : EventSourceListener() {
                override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                    if (data == "[DONE]") {
                        trySend(StreamEvent.Done)
                        channel.close()
                        return
                    }
                    val token = parseToken(data)
                    if (token != null) trySend(StreamEvent.Token(token))
                }
                override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                    trySend(StreamEvent.Error(t?.message ?: "Connection failed"))
                    channel.close()
                }
            })

            awaitClose {
                source.cancel()
                // Notify server to abort generation
                client.newCall(
                    Request.Builder().url("$baseUrl/chat/abort").delete().build()
                ).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: java.io.IOException) {}
                    override fun onResponse(call: Call, response: Response) { response.close() }
                })
            }
        }
        .throttle(maxPerSecond = 20)
        .withThinkingIndicator(timeoutMs = 500)
        .flowOn(Dispatchers.IO)
    }

    private fun buildBody(prompt: PromptRequest): RequestBody {
        val json = """{"messages":${prompt.messages},"temperature":${prompt.temperature},"max_tokens":${prompt.maxTokens}}"""
        return json.toByteArray().let {
            okhttp3.RequestBody.create("application/json".toMediaType(), it)
        }
    }

    private fun parseToken(data: String): String? {
        // Parse {"token": "hello"} — simplified
        return data.substringAfter("\"token\":\"").substringBefore("\"").takeIf { it.isNotEmpty() }
    }
}

// ─── ViewModel ────────────────────────────────────────────────────────────────

class AriaViewModel(private val client: AriaChatClient) : androidx.lifecycle.ViewModel() {
    private var activeStreamJob: Job? = null

    private val _response = MutableStateFlow("")
    val response: StateFlow<String> = _response.asStateFlow()

    private val _isStreaming = MutableStateFlow(false)
    val isStreaming: StateFlow<Boolean> = _isStreaming.asStateFlow()

    fun send(userMessage: String) {
        // Cancel any active stream first
        activeStreamJob?.cancel()
        _response.value = ""

        activeStreamJob = viewModelScope.launch {
            _isStreaming.value = true
            client.stream {
                system("You are Aria, a helpful mobile assistant.")
                user(userMessage)
                temperature(0.7f)
                maxTokens(512)
            }.collect { event ->
                when (event) {
                    is StreamEvent.Token   -> _response.update { it + event.text }
                    is StreamEvent.Done    -> _isStreaming.value = false
                    is StreamEvent.Error   -> { _isStreaming.value = false }
                    is StreamEvent.Thinking -> { /* show thinking indicator */ }
                }
            }
        }
    }

    fun stopStreaming() {
        activeStreamJob?.cancel()
        _isStreaming.value = false
    }
}
```

---

## Unit Test Examples

```kotlin
class ThrottleOperatorTest {
    @Test fun `throttle limits emission rate`() = runTest {
        val emissions = mutableListOf<Long>()
        flowOf(1, 2, 3, 4, 5)
            .throttle(maxPerSecond = 2)
            .collect { emissions.add(System.currentTimeMillis()) }

        val gaps = emissions.zipWithNext { a, b -> b - a }
        gaps.forEach { gap -> assertTrue(gap >= 400L) } // ~500ms between each at 2/sec
    }
}
```

---
---

# Assignment 09 — Bluetooth IoT Sensor Mesh

**Difficulty:** 🟣 Staff  
**Focus:** BLE · Coroutine concurrency · Connection pooling · Kotlin Mutex/Semaphore · Reactive sensor graph

---

## Problem Statement

You're building **SensorGrid**, an Android app that connects to a mesh of Bluetooth Low Energy (BLE) sensors in an industrial facility — temperature, humidity, pressure gauges. The app must discover, connect, and maintain connections to up to 10 sensors simultaneously, stream readings as reactive `Flow`, and handle disconnections gracefully without blocking other sensors.

---

## Functional Requirements

1. `BleScanner` emits `Flow<BleDevice>` of discovered devices
2. `BleConnectionPool` maintains up to `maxConnections` concurrent GATT connections
3. Per-device `Flow<SensorReading>` streams real-time data
4. `Semaphore(maxConnections)` controls concurrent connection attempts (BLE stack limit)
5. Reconnect loop per device: exponential back-off, max 5 retries
6. `SensorAggregator` combines readings from all devices into a single `Flow<AggregatedReading>`

---

## Non-Functional Requirements

- BLE operations are NOT thread-safe — all GATT calls on dedicated `Dispatchers.IO` thread
- Connection pool must release permits on disconnect (no deadlocks)
- `supervisorScope` per device — one crash doesn't kill others
- Memory: bounded `SharedFlow` per device, no unbounded buffering
- Battery: unsubscribed sensors pause notifications (write characteristic)

---

## Constraints & Edge Cases

- Android BLE: max ~7 concurrent GATT connections (device-dependent)
- GATT operations must be sequential per device (no concurrent reads on same connection)
- Device goes out of range → detect via connection timeout, trigger reconnect
- App backgrounded → maintain connections via Foreground Service
- Two devices report same sensor type → aggregate with `median()` not `avg()` (outlier-robust)

---

## Expected Architecture

```
BleScanner
    └── Flow<BleDevice>
            │
    BleConnectionPool (Semaphore)
            ├── BleDeviceConnection [device1]
            │       ├── Mutex (sequential GATT ops)
            │       ├── reconnectLoop()
            │       └── readings: SharedFlow<SensorReading>
            ├── BleDeviceConnection [device2]
            └── ...
                    │
            SensorAggregator
                └── combine(all device flows)
                        └── Flow<AggregatedReading>
```

---

## Full Solution

```kotlin
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.Context
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.*
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

// ─── Domain Models ───────────────────────────────────────────────────────────

data class BleDevice(val address: String, val name: String?, val rssi: Int)

enum class SensorType { TEMPERATURE, HUMIDITY, PRESSURE, CO2 }

data class SensorReading(
    val deviceAddress: String,
    val type: SensorType,
    val value: Float,
    val unit: String,
    val timestamp: Long = System.currentTimeMillis()
)

data class AggregatedReading(
    val type: SensorType,
    val median: Float,
    val min: Float,
    val max: Float,
    val deviceCount: Int
)

sealed class ConnectionState {
    object Connecting : ConnectionState()
    object Connected : ConnectionState()
    data class Disconnected(val reason: String) : ConnectionState()
    data class Failed(val error: String) : ConnectionState()
}

// ─── BLE Scanner ─────────────────────────────────────────────────────────────

class BleScanner(private val context: Context) {
    private val bluetoothLeScanner: BluetoothLeScanner? =
        (context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager)
            ?.adapter?.bluetoothLeScanner

    fun scan(serviceUuid: UUID? = null): Flow<BleDevice> = callbackFlow {
        val filters = serviceUuid?.let {
            listOf(ScanFilter.Builder().setServiceUuid(android.os.ParcelUuid(it)).build())
        }
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                trySend(BleDevice(result.device.address, result.device.name, result.rssi))
            }
            override fun onScanFailed(errorCode: Int) {
                close(Exception("BLE scan failed: $errorCode"))
            }
        }

        bluetoothLeScanner?.startScan(filters, settings, callback)
            ?: close(Exception("Bluetooth not available"))

        awaitClose { bluetoothLeScanner?.stopScan(callback) }
    }.distinctUntilChangedBy { it.address }
     .flowOn(Dispatchers.IO)
}

// ─── Device Connection ────────────────────────────────────────────────────────

class BleDeviceConnection(
    private val context: Context,
    private val device: BleDevice,
    private val scope: CoroutineScope
) {
    // Mutex ensures sequential GATT operations (BLE stack requirement)
    private val gattMutex = Mutex()
    private var gatt: BluetoothGatt? = null

    private val _readings = MutableSharedFlow<SensorReading>(
        replay = 1,
        extraBufferCapacity = 32
    )
    val readings: SharedFlow<SensorReading> = _readings.asSharedFlow()

    private val _state = MutableStateFlow<ConnectionState>(ConnectionState.Connecting)
    val state: StateFlow<ConnectionState> = _state.asStateFlow()

    private var retryCount = 0
    private val maxRetries = 5

    fun start() {
        scope.launch { connectWithRetry() }
    }

    private suspend fun connectWithRetry() {
        while (retryCount <= maxRetries && isActive) {
            try {
                connect()
                retryCount = 0  // reset on success
            } catch (e: CancellationException) {
                throw e  // propagate cancellation
            } catch (e: Exception) {
                retryCount++
                if (retryCount > maxRetries) {
                    _state.value = ConnectionState.Failed("Max retries exceeded")
                    return
                }
                val backoff = minOf(1000L * (1 shl retryCount), 30_000L)
                _state.value = ConnectionState.Disconnected("Retry $retryCount in ${backoff}ms")
                delay(backoff)
            }
        }
    }

    private suspend fun connect() {
        _state.value = ConnectionState.Connecting

        suspendCancellableCoroutine<Unit> { cont ->
            val bluetoothDevice = (context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager)
                .adapter.getRemoteDevice(device.address)

            val callback = object : BluetoothGattCallback() {
                override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
                    when {
                        newState == BluetoothProfile.STATE_CONNECTED -> {
                            gatt = g
                            _state.value = ConnectionState.Connected
                            g.discoverServices()
                        }
                        newState == BluetoothProfile.STATE_DISCONNECTED -> {
                            _state.value = ConnectionState.Disconnected("GATT disconnected")
                            if (cont.isActive) cont.resumeWith(Result.failure(Exception("Disconnected")))
                        }
                    }
                }

                override fun onCharacteristicChanged(g: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                    val reading = parseCharacteristic(characteristic) ?: return
                    scope.launch { _readings.emit(reading) }
                }
            }

            gatt = bluetoothDevice.connectGatt(context, false, callback, BluetoothDevice.TRANSPORT_LE)
            cont.invokeOnCancellation { gatt?.disconnect(); gatt?.close() }
        }
    }

    /** All GATT read/write ops must go through this to ensure sequential access */
    suspend fun <T> withGatt(block: suspend (BluetoothGatt) -> T): T {
        return gattMutex.withLock {
            gatt?.let { block(it) } ?: throw Exception("Not connected")
        }
    }

    fun disconnect() {
        gatt?.disconnect()
        gatt?.close()
        gatt = null
    }

    private fun parseCharacteristic(char: BluetoothGattCharacteristic): SensorReading? {
        // Parse based on UUID — stub
        return SensorReading(device.address, SensorType.TEMPERATURE, 22.5f, "°C")
    }
}

// ─── Connection Pool ──────────────────────────────────────────────────────────

class BleConnectionPool(
    private val context: Context,
    private val maxConnections: Int = 7,
    private val scope: CoroutineScope
) {
    private val semaphore = Semaphore(maxConnections)
    private val connections = ConcurrentHashMap<String, BleDeviceConnection>()

    suspend fun connect(device: BleDevice): BleDeviceConnection {
        if (connections.containsKey(device.address)) {
            return connections[device.address]!!
        }

        semaphore.acquire()  // blocks if pool is full

        val connection = BleDeviceConnection(context, device, scope)
        connections[device.address] = connection

        // Release semaphore when device disconnects/fails
        scope.launch {
            connection.state.collect { state ->
                if (state is ConnectionState.Failed) {
                    connections.remove(device.address)
                    semaphore.release()
                }
            }
        }

        connection.start()
        return connection
    }

    fun disconnect(address: String) {
        connections.remove(address)?.also {
            it.disconnect()
            semaphore.release()
        }
    }

    fun allReadings(): Flow<SensorReading> =
        connections.values
            .map { it.readings }
            .let { flows ->
                if (flows.isEmpty()) emptyFlow()
                else merge(*flows.toTypedArray())
            }
}

// ─── Sensor Aggregator ────────────────────────────────────────────────────────

class SensorAggregator(private val pool: BleConnectionPool) {

    fun aggregatedReadings(): Flow<AggregatedReading> = pool.allReadings()
        .groupBy { it.type }
        .flatMapMerge { (type, readings) ->
            readings.scan(mutableListOf<Float>()) { acc, r ->
                acc.apply { add(r.value) }
            }.map { values ->
                AggregatedReading(
                    type = type,
                    median = values.median(),
                    min = values.minOrNull() ?: 0f,
                    max = values.maxOrNull() ?: 0f,
                    deviceCount = values.size
                )
            }
        }

    private fun List<Float>.median(): Float {
        if (isEmpty()) return 0f
        val sorted = sorted()
        return if (size % 2 == 0) (sorted[size/2 - 1] + sorted[size/2]) / 2f
        else sorted[size / 2]
    }

    // groupBy isn't in stdlib Flow — would use custom operator or flatMapMerge with windowing
    private fun <T, K> Flow<T>.groupBy(keySelector: (T) -> K): Flow<Pair<K, Flow<T>>> = flow {
        // Simplified — real impl uses ConcurrentHashMap of MutableSharedFlows
    }
}
```

---
---

# Assignment 10 — Mobile System Design: Ride-Sharing at Scale

**Difficulty:** 🟣 Staff  
**Focus:** Full system design · Offline-first · Real-time sync · Battery optimization · Privacy · Observability

---

## Problem Statement

Design and implement the core mobile architecture for **ZoomRide** — a production-grade ride-sharing app that must work seamlessly across poor networks, scale to 10M+ daily active users, respect user privacy, optimize battery usage, and provide a snappy UI under all conditions.

This is a Staff-level system design question. You're expected to make and defend architectural decisions, not just write code.

---

## System Design Scope

Design the following subsystems:

### 1. Real-Time Driver Location Tracking

**Requirements:**
- Driver location updates every 3 seconds while ride is active
- Rider sees driver moving on map with smooth interpolation
- Battery-conscious: reduce frequency when app is backgrounded

**Architecture Decision:**

```
Driver Side:
    FusedLocationProviderClient (PRIORITY_HIGH_ACCURACY during ride)
    → LocationUpdate(lat, lng, heading, speed, timestamp)
    → GZip compress → WebSocket → Server

Rider Side:
    WebSocket → LocationRepository → StateFlow<DriverLocation>
    → Compose Map with animated Marker (interpolate between positions)
    → When backgrounded: switch to push notification for ETA updates only

Battery Optimization:
    Foreground: 3s updates (GPS)
    Background: 30s updates (network/cell) via push notification
    Arriving: 1s updates (final 200m)
```

**Implementation:**

```kotlin
class DriverLocationRepository(
    private val wsClient: WebSocketClient,
    private val scope: CoroutineScope
) {
    private val _location = MutableStateFlow<DriverLocation?>(null)
    val location: StateFlow<DriverLocation?> = _location.asStateFlow()

    // Smooth interpolation between GPS points
    val smoothedLocation: Flow<LatLng> = location
        .filterNotNull()
        .scan(Pair<DriverLocation?, DriverLocation?>(null, null)) { (_, prev), curr ->
            Pair(prev, curr)
        }
        .flatMapLatest { (prev, curr) ->
            if (prev == null || curr == null) flowOf(curr!!.toLatLng())
            else interpolate(prev.toLatLng(), curr.toLatLng(), durationMs = 3000)
        }

    private fun interpolate(from: LatLng, to: LatLng, durationMs: Long): Flow<LatLng> = flow {
        val steps = 30
        val stepMs = durationMs / steps
        for (i in 0..steps) {
            val fraction = i.toFloat() / steps
            emit(LatLng(
                from.latitude + (to.latitude - from.latitude) * fraction,
                from.longitude + (to.longitude - from.longitude) * fraction
            ))
            delay(stepMs)
        }
    }
}
```

---

### 2. Offline-First Booking Flow

**Requirements:**
- User can request a ride with no internet (queued)
- App optimistically shows "Finding driver" UI
- Sync when connection returns
- Idempotent: tapping "Book" twice charges once

**Architecture Decision:**

```
User taps "Book"
    │
    ├── Validate locally (destination set, payment method valid)
    ├── Write BookingRequest to Room (status=PENDING, idempotencyKey=UUID)
    ├── Optimistically show "Finding Driver..." UI
    │
    ▼
BookingSyncWorker (WorkManager, CONNECTED constraint)
    │
    ├── POST /bookings { idempotencyKey, origin, destination, vehicleType }
    ├── 200 → update Room (status=CONFIRMED, driverId=...)
    ├── 409 → already booked → fetch booking status → update Room
    ├── 402 → payment failed → status=PAYMENT_FAILED → show error
    └── 5xx → retry with exponential backoff
```

---

### 3. Privacy-First Location Data

**Architecture Decision:**

```
Principles:
    - Location never stored beyond ride duration on device
    - Precise location masked to 100m radius for non-active rides  
    - Location data encrypted at rest (Android Keystore)
    - User can delete location history via "Clear Data" (cascades to Room)

Implementation:
    - Active ride: full precision stored in memory (not Room)
    - Post-ride: only start/end district stored (not exact coordinates)
    - Driver location stream: rounded to 4 decimal places (11m precision)
    - Analytics: differential privacy noise added before sending

data class LocationPrivacy(
    val precision: LocationPrecision,  // EXACT, DISTRICT, CITY
    val retentionMs: Long,             // 0 = memory only
    val encryptAtRest: Boolean
)
```

---

### 4. Multi-Layer Data Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    UI Layer (Compose)                    │
│         collectAsStateWithLifecycle()                    │
└────────────────────────┬────────────────────────────────┘
                         │ StateFlow / PagingData
┌────────────────────────▼────────────────────────────────┐
│                  ViewModel Layer                         │
│     MVI: Intent → Reducer → State + SideEffects         │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                 Repository Layer                         │
│   ┌──────────────┬──────────────┬────────────────────┐  │
│   │ MemoryCache  │  Room (Disk) │  Remote (Retrofit) │  │
│   │  (LRU 100)   │  ← truth →  │  + WebSocket SSE   │  │
│   └──────────────┴──────────────┴────────────────────┘  │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│               Infrastructure Layer                       │
│   WorkManager │ ConnectivityManager │ Android Keystore  │
│   Hilt DI     │ Firebase Crashlytics│ DataStore Prefs   │
└─────────────────────────────────────────────────────────┘
```

---

### 5. Performance Optimization Checklist

```kotlin
// ① Minimize recomposition
@Stable data class RideUiState(...)   // stable = skippable
key(driver.id) { DriverMarker(driver) }  // stable list keys

// ② Lazy initialization
val heavyProcessor by lazy { HeavyProcessor() }  // init only when needed

// ③ Background thread discipline
withContext(Dispatchers.Default) { /* CPU-heavy */ }
withContext(Dispatchers.IO) { /* disk/network */ }
// NEVER block main thread > 16ms

// ④ Image loading
AsyncImage(model = ImageRequest.Builder(context)
    .data(avatarUrl)
    .diskCachePolicy(CachePolicy.ENABLED)
    .memoryCachePolicy(CachePolicy.ENABLED)
    .crossfade(true)
    .build())

// ⑤ Flow operators to prevent overload
driverLocationFlow
    .conflate()           // skip intermediate if UI is slow
    .distinctUntilChanged { a, b -> a.distanceTo(b) < 10 }  // skip if < 10m moved
    .sample(1000)         // max 1 update/sec to map
    .collect { updateMap(it) }

// ⑥ Baseline Profiles
@ExperimentalBaselineProfilesApi
class ZoomRideBaselineProfile : BaselineProfileRule() {
    @Test fun rideBookingFlow() = collectBaselineProfile { startActivity<MainActivity>() }
}
```

---

### 6. Observability & Crash Reporting

```kotlin
// Custom error hierarchy
sealed class ZoomRideError : Exception() {
    data class NetworkError(val code: Int, override val message: String) : ZoomRideError()
    data class BookingError(val reason: BookingFailReason) : ZoomRideError()
    data class LocationError(val cause: String) : ZoomRideError()
    object PaymentDeclined : ZoomRideError()
}

// Structured logging
object Analytics {
    fun track(event: AnalyticsEvent) {
        Firebase.analytics.logEvent(event.name) {
            event.params.forEach { (k, v) -> param(k, v) }
        }
    }
}

sealed class AnalyticsEvent(val name: String, val params: Map<String, String> = emptyMap()) {
    class RideBooked(vehicleType: String, estimatedFare: Double) :
        AnalyticsEvent("ride_booked", mapOf("vehicle" to vehicleType, "fare" to estimatedFare.toString()))
    class BookingFailed(reason: String) :
        AnalyticsEvent("booking_failed", mapOf("reason" to reason))
    class DriverLocationUpdate(latency: Long) :
        AnalyticsEvent("location_latency_ms", mapOf("latency" to latency.toString()))
}
```

---

### 7. Dependency Injection (Hilt)

```kotlin
@Module
@InstallIn(SingletonComponent::class)
object NetworkModule {
    @Provides @Singleton
    fun provideOkHttpClient(authInterceptor: AuthInterceptor): OkHttpClient =
        OkHttpClient.Builder()
            .addInterceptor(authInterceptor)
            .addInterceptor(HttpLoggingInterceptor().apply { level = HttpLoggingInterceptor.Level.BODY })
            .connectTimeout(30, TimeUnit.SECONDS)
            .build()

    @Provides @Singleton
    fun provideRetrofit(client: OkHttpClient): Retrofit =
        Retrofit.Builder()
            .baseUrl(BuildConfig.API_BASE_URL)
            .client(client)
            .addConverterFactory(MoshiConverterFactory.create())
            .build()
}

@Module
@InstallIn(ViewModelComponent::class)
object RepositoryModule {
    @Provides
    fun provideBookingRepository(
        api: BookingApi,
        db: ZoomRideDatabase,
        workManager: WorkManager
    ): BookingRepository = BookingRepository(api, db, workManager)
}

@HiltWorker
class BookingSyncWorker @AssistedInject constructor(
    @Assisted context: Context,
    @Assisted params: WorkerParameters,
    private val repository: BookingRepository  // injected by Hilt
) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result = repository.syncPendingBookings()
}
```

---

## Follow-Up Interview Questions (Staff-Level)

1. **Conflict resolution:** Two devices book the same driver simultaneously. The server uses optimistic locking. How does your client handle `409 Conflict` vs `200 OK`?

2. **Scale:** Your app has 10M DAU. The WebSocket server receives 3 location updates/sec/driver. How do you architect the backend to handle this? (hint: CQRS, event streaming, Kafka)

3. **Privacy:** A government requests all location data for a user from the past 30 days. What's your data minimization strategy that satisfies both GDPR and the request?

4. **Battery:** On a 4-hour ride, your app drains 15% battery from location tracking. How do you reduce this to < 5% without degrading UX?

5. **Testing:** How do you write an integration test that simulates a 5-minute network outage mid-ride and verifies the booking syncs correctly when connectivity returns?

6. **Rollout:** You're releasing a schema migration that renames a Room column. How do you deploy this safely to 10M users with zero data loss? Write the migration.

7. **Performance:** Layout Inspector shows `DriverMarker` recomposes 60 times/second. Diagnose the root cause and fix it using only Compose APIs.

8. **Security:** Your API key is hardcoded in `BuildConfig`. A security audit flags this. What's the correct solution for mobile apps? (Certificate pinning, ProGuard, Android Keystore)

---

## Common Staff-Level Mistakes

- **Single point of failure:** WebSocket only — no fallback to HTTP long-polling when WS is blocked by corporate firewalls
- **No graceful degradation:** App crashes when offline instead of serving cached state
- **Uncontrolled parallelism:** `launch {}` in loops without Semaphore or Channel — exhausts thread pool
- **Overly broad `catch (e: Exception)`:** Swallows `CancellationException`, breaks coroutine cancellation
- **Shared mutable state without synchronization:** `mutableListOf()` accessed from multiple coroutines without `Mutex`
- **Missing baseline profiles:** Cold start is 3x slower than it needs to be
- **No observability:** No structured logging — can't debug production incidents from logs alone
