# Assignment 03 — Live Sports Score Ticker

**Difficulty:** 🟡 Medium  
**Focus:** StateFlow · SharedFlow · Jetpack Compose · Lifecycle-aware collection · Multi-screen broadcasting

---

## Problem Statement

You're building **ScoreZone**, a live sports app. Multiple screens — the home feed, a match detail view, and a notification widget — all need to display the same real-time score. Scores arrive from a WebSocket and must be broadcast to all active subscribers without duplication or missed updates.

---

## Real-World Scenario

A user opens the app during a match. The home feed shows a compact score chip. They navigate into the match detail view for a full scoreboard. A persistent notification also updates live. All three must stay in sync from a single upstream source — no triple-fetching, no memory leaks when screens are backgrounded.

---

## Functional Requirements

1. `ScoreRepository` connects to a WebSocket and emits `ScoreUpdate` events
2. `StateFlow` holds the **current score** for each active match (survive configuration change)
3. `SharedFlow` broadcasts **score events** (goals, cards, substitutions) to subscribers
4. `MatchViewModel` exposes both to Compose UI
5. Compose screens collect flows lifecycle-safely with `collectAsStateWithLifecycle`
6. Background screens pause collection (no wasted processing)

---

## Non-Functional Requirements

- Single WebSocket connection shared across all screens (no duplicates)
- Score state survives screen rotation
- Event flow drops old events when buffer full (backpressure strategy: `DROP_OLDEST`)
- ViewModel survives navigation between match list ↔ detail
- Zero memory leaks: flows cancelled when ViewModel cleared

---

## Constraints & Edge Cases

- Match ends → emit `MatchEvent.FullTime`, stop emitting scores
- WebSocket disconnects → retry with exponential back-off, expose `ConnectionState`
- Multiple matches running simultaneously → keyed `StateFlow` per matchId
- User opens same match on two tabs (multi-window) → both update from same flow

---

## Expected Architecture

```
WebSocketService (Singleton / Hilt)
    └── Flow<RawScoreMessage>
            │
            ▼
    ScoreRepository
        ├── matchScores: Map<MatchId, StateFlow<ScoreState>>   ← current score per match
        └── matchEvents: SharedFlow<MatchEvent>                ← goals, cards, FT whistle
                │
                ▼
        MatchViewModel (per match screen)
            ├── scoreState: StateFlow<ScoreUiState>
            └── eventBus: SharedFlow<MatchEvent>
                    │
                    ▼
            Compose UI
                ├── ScoreChipScreen   (home feed)
                ├── MatchDetailScreen (full scoreboard)
                └── ScoreNotificationWidget
```

---

## Sample Input/Output

```kotlin
// Repository emits:
ScoreState(matchId = "m1", home = "Arsenal", away = "Chelsea", homeGoals = 2, awayGoals = 1)

// UI collects via ViewModel:
val score by viewModel.scoreState.collectAsStateWithLifecycle()

Text("${score.homeGoals} - ${score.awayGoals}")  // "2 - 1"

// Event (one-time):
MatchEvent.Goal(matchId = "m1", team = Team.Home, scorer = "Saka", minute = 67)
// → Snackbar: "⚽ GOAL! Saka (67')"
```

---

## Suggested APIs / Classes

```kotlin
stateIn(scope, SharingStarted.WhileSubscribed(5000), initialValue)
shareIn(scope, SharingStarted.Eagerly, replay = 0)
collectAsStateWithLifecycle()   // androidx.lifecycle:lifecycle-runtime-compose
MutableStateFlow / MutableSharedFlow
viewModelScope.launch { }
SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000)
```

---

## Follow-Up Interview Questions

1. Why `StateFlow` for current score and `SharedFlow` for events? Could you use just one?
2. What does `SharingStarted.WhileSubscribed(5000)` mean? Why 5 seconds specifically?
3. `collectAsState()` vs `collectAsStateWithLifecycle()` — what's the difference and when does it matter?
4. If the ViewModel is shared between two Compose destinations, who owns the WebSocket lifecycle?
5. How would you handle a "goal replay" animation triggered by `SharedFlow` exactly once per subscriber?
6. What happens to events emitted while the screen is in the background with `replay = 0`?

---

## Common Mistakes

- Using `SharedFlow` for current score (new subscribers miss the latest value)
- Using `StateFlow` for one-time events (replay causes re-showing snackbars on rotation)
- Calling `flow.collect {}` directly in a Composable without `LaunchedEffect`
- Forgetting `SharingStarted.WhileSubscribed` → upstream keeps running in background
- Sharing `MutableStateFlow` directly from Repository (breaks encapsulation)

---

## Optimization Discussion

**`SharingStarted.WhileSubscribed(5_000)`:** Keeps the upstream alive 5 seconds after last subscriber leaves. Prevents reconnect on fast screen rotation (rotation takes ~300ms). Tune this per use case.  
**`replay` buffer:** `replay = 1` on `SharedFlow` for events means new subscribers get the last event — useful for "current status" but wrong for one-time toasts.  
**Conflation:** For score chips that update very fast (tennis, cricket), use `conflate()` so the UI only processes the latest score when it catches up.

---

## Full Solution

```kotlin
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

// ─── Domain Models ───────────────────────────────────────────────────────────

data class ScoreState(
    val matchId: String,
    val home: String,
    val away: String,
    val homeGoals: Int = 0,
    val awayGoals: Int = 0,
    val minute: Int = 0,
    val isLive: Boolean = true
)

sealed class MatchEvent {
    abstract val matchId: String
    data class Goal(override val matchId: String, val team: Team, val scorer: String, val minute: Int) : MatchEvent()
    data class YellowCard(override val matchId: String, val player: String, val minute: Int) : MatchEvent()
    data class RedCard(override val matchId: String, val player: String, val minute: Int) : MatchEvent()
    data class FullTime(override val matchId: String, val homeGoals: Int, val awayGoals: Int) : MatchEvent()
}

enum class Team { Home, Away }

sealed class ConnectionState {
    object Connected : ConnectionState()
    object Reconnecting : ConnectionState()
    data class Error(val message: String) : ConnectionState()
}

data class ScoreUiState(
    val score: ScoreState? = null,
    val connectionState: ConnectionState = ConnectionState.Reconnecting,
    val lastEvent: MatchEvent? = null
)

// ─── Fake WebSocket Service (Singleton) ──────────────────────────────────────

object WebSocketService {
    /** Simulates incoming score messages from a server */
    fun scoreStream(matchId: String): Flow<ScoreState> = flow {
        var homeGoals = 0
        var awayGoals = 0
        for (minute in 1..90) {
            delay(100) // simulate real-time ticks
            if (minute == 23) homeGoals++
            if (minute == 45) awayGoals++
            if (minute == 67) homeGoals++
            emit(ScoreState(matchId, "Arsenal", "Chelsea", homeGoals, awayGoals, minute))
        }
    }.flowOn(Dispatchers.IO)

    fun eventStream(matchId: String): Flow<MatchEvent> = flow {
        delay(2300)
        emit(MatchEvent.Goal(matchId, Team.Home, "Saka", 23))
        delay(2200)
        emit(MatchEvent.Goal(matchId, Team.Away, "Palmer", 45))
        delay(2200)
        emit(MatchEvent.Goal(matchId, Team.Home, "Martinelli", 67))
        delay(2300)
        emit(MatchEvent.FullTime(matchId, 2, 1))
    }.flowOn(Dispatchers.IO)
}

// ─── Repository ───────────────────────────────────────────────────────────────

class ScoreRepository(
    private val externalScope: CoroutineScope  // Application-level scope
) {
    private val _scoreFlows = mutableMapOf<String, StateFlow<ScoreState?>>()
    private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Reconnecting)

    // Shared event bus — all screens share one flow
    private val _matchEvents = MutableSharedFlow<MatchEvent>(
        replay = 0,
        extraBufferCapacity = 64,
        onBufferOverflow = kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
    )
    val matchEvents: SharedFlow<MatchEvent> = _matchEvents.asSharedFlow()
    val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

    fun scoreStateFor(matchId: String): StateFlow<ScoreState?> =
        _scoreFlows.getOrPut(matchId) {
            WebSocketService.scoreStream(matchId)
                .onStart { _connectionState.value = ConnectionState.Connected }
                .catch { e ->
                    _connectionState.value = ConnectionState.Error(e.message ?: "Unknown")
                }
                .stateIn(
                    scope = externalScope,
                    started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
                    initialValue = null
                )
        }

    fun startEventStream(matchId: String) {
        externalScope.launch {
            WebSocketService.eventStream(matchId).collect { event ->
                _matchEvents.emit(event)
            }
        }
    }
}

// ─── ViewModel ────────────────────────────────────────────────────────────────

class MatchViewModel(
    private val matchId: String,
    private val repository: ScoreRepository
) : ViewModel() {

    // Current score — StateFlow so new collectors immediately get the last value
    val scoreState: StateFlow<ScoreUiState> = repository.scoreStateFor(matchId)
        .combine(repository.connectionState) { score, connection ->
            ScoreUiState(score = score, connectionState = connection)
        }
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = ScoreUiState()
        )

    // One-time events — SharedFlow so rotation doesn't re-show snackbars
    val matchEvents: SharedFlow<MatchEvent> = repository.matchEvents
        .filter { it.matchId == matchId }
        .shareIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            replay = 0  // No replay — events are one-shot
        )

    init {
        repository.startEventStream(matchId)
    }
}

// ─── Compose UI ───────────────────────────────────────────────────────────────

/*
@Composable
fun MatchDetailScreen(viewModel: MatchViewModel = hiltViewModel()) {
    val uiState by viewModel.scoreState.collectAsStateWithLifecycle()
    val snackbarHostState = remember { SnackbarHostState() }

    // Collect one-time events safely
    LaunchedEffect(Unit) {
        viewModel.matchEvents.collect { event ->
            when (event) {
                is MatchEvent.Goal -> snackbarHostState.showSnackbar(
                    "⚽ GOAL! ${event.scorer} (${event.minute}')"
                )
                is MatchEvent.FullTime -> snackbarHostState.showSnackbar("Full time!")
                else -> {}
            }
        }
    }

    Scaffold(snackbarHost = { SnackbarHost(snackbarHostState) }) { padding ->
        uiState.score?.let { score ->
            Column(modifier = Modifier.padding(padding).fillMaxSize(),
                   horizontalAlignment = Alignment.CenterHorizontally) {
                Text(score.home, style = MaterialTheme.typography.headlineMedium)
                Text("${score.homeGoals}  —  ${score.awayGoals}",
                    style = MaterialTheme.typography.displayLarge)
                Text(score.away, style = MaterialTheme.typography.headlineMedium)
                Text("${score.minute}'", color = Color.Red)
            }
        } ?: CircularProgressIndicator()
    }
}
*/

// ─── Standalone Demo ──────────────────────────────────────────────────────────

fun main() = runBlocking {
    val appScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val repo = ScoreRepository(appScope)

    val scoreFlow = repo.scoreStateFor("m1")

    val collector1 = launch {
        scoreFlow.filterNotNull().collect { score ->
            println("[Feed]   Arsenal ${score.homeGoals}-${score.awayGoals} Chelsea (${score.minute}')")
        }
    }

    val collector2 = launch {
        scoreFlow.filterNotNull().collect { score ->
            println("[Detail] ${score.homeGoals} - ${score.awayGoals}")
        }
    }

    val eventCollector = launch {
        repo.matchEvents.collect { event ->
            when (event) {
                is MatchEvent.Goal     -> println("⚽ GOAL! ${event.scorer} (${event.minute}')")
                is MatchEvent.FullTime -> println("🏁 Full Time! ${event.homeGoals}-${event.awayGoals}")
                else -> {}
            }
        }
    }

    repo.startEventStream("m1")
    delay(10_000)

    appScope.cancel()
}
```

---

## Alternative Approaches & Tradeoffs

| Approach | Pro | Con |
|---|---|---|
| Single `SharedFlow` for everything | Simple | New screens miss current score |
| `LiveData` + `MediatorLiveData` | Lifecycle-aware out of the box | Not Kotlin-idiomatic, no back-pressure |
| `BroadcastChannel` (deprecated) | Was the original approach | Replaced by `SharedFlow` |
| `combine()` in ViewModel | Declarative composition | Can become complex with many inputs |

---

## Unit Test Examples

```kotlin
import kotlinx.coroutines.test.*
import kotlinx.coroutines.flow.first
import org.junit.Test
import org.junit.Assert.*

class ScoreRepositoryTest {

    @Test
    fun `score state reflects latest score from stream`() = runTest {
        val scope = CoroutineScope(this.coroutineContext)
        val repo = ScoreRepository(scope)
        val flow = repo.scoreStateFor("m1")

        // Advance time to get first emission
        val state = flow.filterNotNull().first()
        assertNotNull(state)
        assertEquals("m1", state.matchId)
    }

    @Test
    fun `match events are broadcast to all collectors`() = runTest {
        val scope = CoroutineScope(this.coroutineContext)
        val repo = ScoreRepository(scope)

        val events1 = mutableListOf<MatchEvent>()
        val events2 = mutableListOf<MatchEvent>()

        val j1 = launch { repo.matchEvents.collect { events1.add(it) } }
        val j2 = launch { repo.matchEvents.collect { events2.add(it) } }

        repo.startEventStream("m1")
        advanceTimeBy(3000)

        assertTrue(events1.size == events2.size)
        j1.cancel(); j2.cancel()
    }
}
```
