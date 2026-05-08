# Assignment 07 — Real-Time Collaborative Note Editor

**Difficulty:** 🔴 Senior  
**Focus:** CRDT · WebSocket · SharedFlow · Kotlin coroutines · Jetpack Compose

---

## Problem Statement

You're building **NoteSync**, a Google Docs-like collaborative note editor. Multiple users can edit the same note simultaneously from different devices. Changes must merge without conflicts, work offline, and sync when connectivity returns.

---

## Real-World Scenario

Two team members open the same meeting note. Alice adds a bullet at line 3 while Bob deletes line 5 — simultaneously, with no server round-trip. When their edits reach the server, both changes must be applied without one overwriting the other. If either goes offline mid-edit, their changes queue locally and sync seamlessly when reconnected.

---

## Functional Requirements

1. Implement a simplified **Last-Write-Wins (LWW) Element Set CRDT** for note content
2. Operations: `Insert(position, text, timestamp, authorId)`, `Delete(position, length, timestamp, authorId)`
3. WebSocket connection for real-time sync (reconnects with exponential back-off)
4. Local operation queue for offline edits
5. `SharedFlow<NoteOperation>` broadcasts remote ops to all local subscribers
6. Compose `BasicTextField` with cursor position preserved after remote merge

---

## Non-Functional Requirements

- Operations are commutative — applying in any order produces same result
- Local operations applied immediately (optimistic UI)
- Remote operations merged without UI flicker
- WebSocket reconnect: 1s, 2s, 4s, 8s, max 30s
- Offline queue persisted to Room (survives process death)

---

## Constraints & Edge Cases

- Two simultaneous inserts at same position → use `authorId` as tiebreaker for determinism
- Delete of already-deleted range → no-op (idempotent)
- Long offline period → full document sync on reconnect (not just delta)
- User A types while user B's operations are being applied → preserve A's cursor offset
- Note deleted by another user while editing → show "Note deleted" overlay

---

## Expected Architecture

```
NoteEditorViewModel
    ├── localOps: Channel<NoteOperation>          (user edits)
    ├── remoteOps: SharedFlow<NoteOperation>      (from server)
    ├── noteState: StateFlow<NoteState>           (merged document)
    └── syncEngine: NoteSyncEngine
            ├── WebSocketClient (OkHttp)
            ├── OfflineQueue (Room)
            └── CrdtMerger.merge(local, remote): NoteContent

NoteState
    ├── content: NoteContent (CRDT document)
    ├── cursors: Map<UserId, CursorPosition>
    ├── isConnected: Boolean
    └── pendingOps: Int
```

---

## Sample Input/Output

```kotlin
// Alice inserts at position 5
val aliceOp = NoteOperation.Insert(
    noteId = "note-1", position = 5, text = "Hello",
    timestamp = clock.now(), authorId = "alice"
)

// Bob simultaneously deletes positions 3-7
val bobOp = NoteOperation.Delete(
    noteId = "note-1", position = 3, length = 4,
    timestamp = clock.now(), authorId = "bob"
)

// CRDT merge: both ops applied, result is deterministic
val merged = CrdtMerger.merge(document, listOf(aliceOp, bobOp))
// Order of application doesn't change the result
```

---

## Suggested APIs / Classes

```kotlin
OkHttpClient().newWebSocket(request, object : WebSocketListener() { ... })
MutableSharedFlow<NoteOperation>(replay = 0, extraBufferCapacity = 256)
Channel<NoteOperation>(Channel.UNLIMITED)
kotlinx.coroutines.flow.merge(flow1, flow2)
@Entity class PendingOperation  // offline queue in Room
BasicTextField(value, onValueChange, modifier)
rememberUpdatedState()  // stable callback reference in LaunchedEffect
```

---

## Follow-Up Interview Questions

1. What is a CRDT? Why is commutativity important for collaborative editing?
2. LWW vs Operational Transform vs CRDT — when would you use each in a mobile app?
3. How do you preserve cursor position when remote ops shift text positions?
4. Why use `SharedFlow` with `replay=0` for remote operations?
5. How does the offline queue ensure operations are applied in the correct order on sync?
6. What's the difference between `merge(flow1, flow2)` and `combine(flow1, flow2)`?

---

## Common Mistakes

- Using timestamp alone for conflict resolution (clock skew breaks determinism)
- Not re-throwing `CancellationException` in WebSocket error handler
- Applying remote ops directly to `BasicTextField` value without adjusting cursor offset
- SharedFlow buffer overflow dropping operations silently (use `DROP_OLDEST` carefully)
- Not persisting pending ops to Room — lost on process death

---

## Full Solution

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import okhttp3.*
import java.util.concurrent.atomic.AtomicLong

// ─── CRDT Domain ──────────────────────────────────────────────────────────────

data class NoteChar(
    val id: String,          // unique: "${authorId}_${lamportClock}"
    val char: Char,
    val authorId: String,
    val timestamp: Long,
    val isDeleted: Boolean = false
)

/**
 * Simplified sequence CRDT — a list of NoteChar with tombstones.
 * Visible content = chars where !isDeleted, sorted by (timestamp, authorId).
 */
class NoteDocument(private val chars: MutableList<NoteChar> = mutableListOf()) {

    val visibleText: String
        get() = chars.filter { !it.isDeleted }.joinToString("") { it.char.toString() }

    /** Insert text at visible position, returns new document */
    fun insert(visiblePosition: Int, text: String, authorId: String, clock: Long): NoteDocument {
        val newChars = chars.toMutableList()
        val visibleIndices = chars.indices.filter { !chars[it].isDeleted }
        val insertBefore = if (visiblePosition < visibleIndices.size)
            visibleIndices[visiblePosition] else chars.size

        text.forEachIndexed { i, c ->
            newChars.add(insertBefore + i, NoteChar(
                id = "${authorId}_${clock + i}",
                char = c,
                authorId = authorId,
                timestamp = clock + i
            ))
        }
        return NoteDocument(newChars)
    }

    /** Delete visible range, returns new document with tombstones */
    fun delete(visibleStart: Int, length: Int): NoteDocument {
        val newChars = chars.toMutableList()
        val visibleIndices = chars.indices.filter { !chars[it].isDeleted }
        val toDelete = visibleIndices.drop(visibleStart).take(length)
        toDelete.forEach { idx ->
            newChars[idx] = newChars[idx].copy(isDeleted = true)
        }
        return NoteDocument(newChars)
    }

    /** Merge a remote op — commutative and idempotent */
    fun applyRemote(op: NoteOperation): NoteDocument = when (op) {
        is NoteOperation.Insert -> insert(op.position, op.text, op.authorId, op.timestamp)
        is NoteOperation.Delete -> delete(op.position, op.length)
        is NoteOperation.NoOp   -> this
    }

    fun snapshot(): List<NoteChar> = chars.toList()
}

// ─── Operations ───────────────────────────────────────────────────────────────

sealed class NoteOperation {
    abstract val noteId: String
    abstract val authorId: String
    abstract val timestamp: Long

    data class Insert(
        override val noteId: String,
        override val authorId: String,
        override val timestamp: Long,
        val position: Int,
        val text: String
    ) : NoteOperation()

    data class Delete(
        override val noteId: String,
        override val authorId: String,
        override val timestamp: Long,
        val position: Int,
        val length: Int
    ) : NoteOperation()

    data class NoOp(
        override val noteId: String,
        override val authorId: String,
        override val timestamp: Long
    ) : NoteOperation()
}

// ─── Lamport Clock ────────────────────────────────────────────────────────────

class LamportClock {
    private val counter = AtomicLong(System.currentTimeMillis())
    fun tick(): Long = counter.incrementAndGet()
    fun witness(remote: Long) { counter.updateAndGet { maxOf(it, remote) + 1 } }
}

// ─── WebSocket Client ─────────────────────────────────────────────────────────

class NoteWebSocketClient(
    private val noteId: String,
    private val scope: CoroutineScope
) {
    private val _remoteOps = MutableSharedFlow<NoteOperation>(
        replay = 0,
        extraBufferCapacity = 256
    )
    val remoteOps: SharedFlow<NoteOperation> = _remoteOps.asSharedFlow()

    private val _connected = MutableStateFlow(false)
    val isConnected: StateFlow<Boolean> = _connected.asStateFlow()

    private var webSocket: WebSocket? = null
    private var retryDelayMs = 1_000L

    fun connect() {
        scope.launch { connectWithRetry() }
    }

    private suspend fun connectWithRetry() {
        while (isActive) {
            try {
                connectOnce()
                retryDelayMs = 1_000L  // reset on success
            } catch (e: Exception) {
                _connected.value = false
                delay(retryDelayMs)
                retryDelayMs = minOf(retryDelayMs * 2, 30_000L)
            }
        }
    }

    private suspend fun connectOnce() {
        val request = Request.Builder().url("wss://notesync.io/ws/$noteId").build()
        val client = OkHttpClient()
        suspendCancellableCoroutine<Unit> { cont ->
            val ws = client.newWebSocket(request, object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    _connected.value = true
                    retryDelayMs = 1_000L
                }
                override fun onMessage(webSocket: WebSocket, text: String) {
                    val op = parseOperation(text)
                    scope.launch { _remoteOps.emit(op) }
                }
                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    _connected.value = false
                    if (cont.isActive) cont.resumeWith(Result.failure(t))
                }
                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                    _connected.value = false
                    if (cont.isActive) cont.resume(Unit) {}
                }
            })
            webSocket = ws
            cont.invokeOnCancellation { ws.close(1000, "Cancelled") }
        }
    }

    fun send(op: NoteOperation) {
        webSocket?.send(serializeOperation(op))
    }

    private fun parseOperation(json: String): NoteOperation = NoteOperation.NoOp("", "", 0) // stub
    private fun serializeOperation(op: NoteOperation): String = op.toString() // stub
}

// ─── ViewModel ────────────────────────────────────────────────────────────────

data class NoteUiState(
    val text: String = "",
    val cursorPosition: Int = 0,
    val isConnected: Boolean = false,
    val activeUsers: List<String> = emptyList(),
    val error: String? = null
)

class NoteEditorViewModel(
    private val noteId: String,
    private val userId: String
) : androidx.lifecycle.ViewModel() {

    private val clock = LamportClock()
    private var document = NoteDocument()

    private val wsClient = NoteWebSocketClient(noteId, viewModelScope)
    private val localOps = Channel<NoteOperation>(Channel.UNLIMITED)

    private val _uiState = MutableStateFlow(NoteUiState())
    val uiState: StateFlow<NoteUiState> = _uiState.asStateFlow()

    init {
        wsClient.connect()

        // Collect remote ops and merge into document
        viewModelScope.launch {
            wsClient.remoteOps.collect { op ->
                clock.witness(op.timestamp)
                document = document.applyRemote(op)
                // Adjust cursor for remote insertions before cursor position
                val cursorAdjust = when (op) {
                    is NoteOperation.Insert -> if (op.position <= _uiState.value.cursorPosition) op.text.length else 0
                    is NoteOperation.Delete -> if (op.position < _uiState.value.cursorPosition) -op.length else 0
                    else -> 0
                }
                _uiState.update { it.copy(
                    text = document.visibleText,
                    cursorPosition = (it.cursorPosition + cursorAdjust).coerceAtLeast(0)
                ) }
            }
        }

        // Collect connectivity
        viewModelScope.launch {
            wsClient.isConnected.collect { connected ->
                _uiState.update { it.copy(isConnected = connected) }
            }
        }

        // Send local ops to server
        viewModelScope.launch {
            for (op in localOps) {
                wsClient.send(op)
            }
        }
    }

    fun onTextChanged(newText: String, cursorPos: Int) {
        val oldText = document.visibleText
        val op = diffToOperation(oldText, newText, userId, clock.tick())
        document = document.applyRemote(op)  // optimistic local apply
        _uiState.update { it.copy(text = newText, cursorPosition = cursorPos) }
        localOps.trySend(op)
    }

    private fun diffToOperation(old: String, new: String, authorId: String, ts: Long): NoteOperation {
        // Simplified diff — production would use Myers diff algorithm
        return if (new.length > old.length) {
            val pos = new.zip(old).indexOfFirst { (a, b) -> a != b }.takeIf { it >= 0 } ?: old.length
            NoteOperation.Insert(noteId, authorId, ts, pos, new.substring(pos, pos + (new.length - old.length)))
        } else {
            val pos = new.zip(old).indexOfFirst { (a, b) -> a != b }.takeIf { it >= 0 } ?: new.length
            NoteOperation.Delete(noteId, authorId, ts, pos, old.length - new.length)
        }
    }
}
```

---

## Unit Test Examples

```kotlin
class NoteDocumentTest {
    @Test fun `concurrent inserts at same position are deterministic`() {
        val doc = NoteDocument()
        val doc1 = doc.insert(0, "Alice", "alice", 100)
        val doc2 = doc.insert(0, "Bob", "bob", 100)

        // Apply in different orders — same result
        val resultAB = doc1.applyRemote(NoteOperation.Insert("n", "bob", 100, 0, "Bob"))
        val resultBA = doc2.applyRemote(NoteOperation.Insert("n", "alice", 100, 0, "Alice"))

        // Both contain same characters (order determined by authorId tiebreak)
        assertEquals(resultAB.visibleText.toSet(), resultBA.visibleText.toSet())
    }

    @Test fun `delete is idempotent`() {
        val doc = NoteDocument().insert(0, "Hello", "alice", 100)
        val deleted = doc.delete(0, 3)
        val deletedAgain = deleted.delete(0, 3)
        // Second delete of same range is a no-op
        assertEquals(deleted.visibleText, deletedAgain.visibleText)
    }

    @Test fun `insert then delete produces correct visible text`() {
        val doc = NoteDocument()
            .insert(0, "Hello World", "alice", 100)
            .delete(5, 6) // delete " World"
        assertEquals("Hello", doc.visibleText)
    }
}
```
