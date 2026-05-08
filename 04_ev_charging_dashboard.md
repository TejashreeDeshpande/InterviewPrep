# Assignment 04 — EV Charging Station Dashboard

**Difficulty:** 🟡 Medium  
**Focus:** Jetpack Compose · MVI Architecture · StateFlow · Animated UI · Side Effects

---

## Problem Statement

You're building **VoltDash**, an EV charging station monitoring app. Fleet managers need a real-time dashboard showing charging status, battery levels, power draw, and alerts across multiple vehicles — all in a responsive Compose UI driven by a clean MVI loop.

---

## Real-World Scenario

A fleet operator monitors 8 EVs simultaneously. Each vehicle's charging state updates every 2 seconds. The dashboard must show animated battery fill gauges, color-coded status chips, a power consumption chart, and alert banners when a vehicle has a fault. User actions (refresh, dismiss alert, select vehicle) flow through a single `Intent` channel.

---

## Functional Requirements

1. `VehicleIntent` sealed class covers: `Refresh`, `SelectVehicle(id)`, `DismissAlert(id)`, `ToggleCharging(id)`
2. `VehicleUiState` is a single immutable data class (no multiple LiveData fields)
3. Reducer function: `(State, Intent) -> State` — pure, testable
4. Side effects handled in ViewModel (API calls, navigation) — not in reducer
5. Battery level shown as animated horizontal progress bar (animate from old % to new %)
6. Status color: `Charging`=green, `Fault`=red, `Complete`=blue, `Idle`=gray

---

## Non-Functional Requirements

- Reducer must be a pure function (no coroutines, no I/O)
- State updates trigger minimal Compose recompositions (use `key {}` and `@Stable`)
- Animations use `animateFloatAsState` with `spring` spec
- ViewModel processes intents sequentially (no race conditions)
- Works on both phone (single column) and tablet (grid)

---

## Constraints & Edge Cases

- 0 vehicles connected → show `EmptyState` composable
- All vehicles faulted → show global alert banner
- Battery at 100% and still "charging" → auto-emit `ToggleCharging` intent
- Refresh while already loading → debounce/ignore duplicate refresh
- Vehicle removed mid-session → gracefully remove from state

---

## Expected Architecture

```
UI (Compose)
    │  VehicleIntent (user actions)
    ▼
MatchViewModel
    ├── intentChannel: Channel<VehicleIntent>
    ├── reduce(state, intent): VehicleUiState   ← pure function
    ├── handleSideEffect(intent)                ← coroutines, API calls
    └── uiState: StateFlow<VehicleUiState>
            │
            ▼
    VehicleUiState
        ├── vehicles: List<VehicleState>
        ├── selectedVehicleId: String?
        ├── isLoading: Boolean
        ├── alerts: List<Alert>
        └── error: String?
```

---

## Sample Input/Output

```kotlin
// User taps "Dismiss Alert" on vehicle "v3"
viewModel.dispatch(VehicleIntent.DismissAlert("v3"))

// State transition:
before: VehicleUiState(alerts = [Alert("v3", "Overheat"), Alert("v1", "Low power")])
after:  VehicleUiState(alerts = [Alert("v1", "Low power")])

// Vehicle at 100%:
VehicleState(id="v2", batteryPct=100, status=ChargingStatus.Charging)
→ auto-intent dispatched: VehicleIntent.ToggleCharging("v2")
→ VehicleState(id="v2", status=ChargingStatus.Complete)
```

---

## Suggested APIs / Classes

```kotlin
animateFloatAsState(targetValue, animationSpec = spring(dampingRatio = Spring.DampingRatioMediumBouncy))
LaunchedEffect(vehicleId) { }      // for per-vehicle side effects
@Stable data class VehicleState    // hint Compose this is stable
key(vehicle.id) { VehicleCard(vehicle) }   // stable list recomposition
Channel<VehicleIntent>(UNLIMITED)
viewModelScope.launch { for (intent in channel) { ... } }
```

---

## Follow-Up Interview Questions

1. Why is the reducer a pure function? How does this help with testing?
2. How do you prevent the intent channel from causing race conditions when two intents arrive simultaneously?
3. `animateFloatAsState` vs `Animatable` — when would you use each?
4. How would you add undo support (e.g., undo dismiss alert)?
5. Why mark `VehicleState` as `@Stable`? What does Compose do differently with stable types?
6. How would you unit test a MVI reducer without spinning up a ViewModel?

---

## Common Mistakes

- Putting coroutines/API calls inside the reducer (breaks purity)
- Using multiple `MutableStateFlow` fields instead of a single `UiState` (split-brain state)
- Missing `key {}` on list items → full list recomposition on every update
- Collecting `StateFlow` in a Composable without `collectAsStateWithLifecycle` → memory leaks
- Triggering animation inside `LaunchedEffect` that restarts on every recomposition

---

## Optimization Discussion

**Minimal recomposition:** Use `@Stable` on data classes and `key(id)` in `LazyColumn`. Profile with Layout Inspector → Recomposition Counts.  
**Batching intents:** If many sensor updates arrive per second, batch them in the ViewModel before emitting to state: `debounce(100ms)` on the intent channel.  
**Derived state:** Use `val criticalVehicles = remember { derivedStateOf { vehicles.filter { it.status == Fault } } }` for expensive filtered lists — only recomputes when `vehicles` changes.

---

## Full Solution

```kotlin
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

// ─── Domain Models ───────────────────────────────────────────────────────────

enum class ChargingStatus { Charging, Complete, Fault, Idle }

@androidx.compose.runtime.Stable
data class VehicleState(
    val id: String,
    val name: String,
    val batteryPct: Float,       // 0f..1f
    val powerKw: Double,
    val status: ChargingStatus,
    val hasAlert: Boolean = false,
    val alertMessage: String? = null
)

data class VehicleUiState(
    val vehicles: List<VehicleState> = emptyList(),
    val selectedVehicleId: String? = null,
    val isLoading: Boolean = false,
    val error: String? = null,
    val totalPowerKw: Double = 0.0
) {
    val selectedVehicle: VehicleState?
        get() = vehicles.find { it.id == selectedVehicleId }

    val criticalCount: Int
        get() = vehicles.count { it.status == ChargingStatus.Fault }
}

// ─── MVI Intents ─────────────────────────────────────────────────────────────

sealed class VehicleIntent {
    object Refresh : VehicleIntent()
    data class SelectVehicle(val id: String) : VehicleIntent()
    data class DismissAlert(val id: String) : VehicleIntent()
    data class ToggleCharging(val id: String) : VehicleIntent()
    data class VehicleUpdated(val vehicle: VehicleState) : VehicleIntent()
    data class LoadSuccess(val vehicles: List<VehicleState>) : VehicleIntent()
    data class LoadFailed(val error: String) : VehicleIntent()
}

// ─── Pure Reducer ─────────────────────────────────────────────────────────────

object VehicleReducer {
    fun reduce(state: VehicleUiState, intent: VehicleIntent): VehicleUiState = when (intent) {
        is VehicleIntent.Refresh -> state.copy(isLoading = true, error = null)

        is VehicleIntent.LoadSuccess -> state.copy(
            vehicles = intent.vehicles,
            isLoading = false,
            totalPowerKw = intent.vehicles.sumOf { it.powerKw }
        )

        is VehicleIntent.LoadFailed -> state.copy(
            isLoading = false,
            error = intent.error
        )

        is VehicleIntent.SelectVehicle -> state.copy(selectedVehicleId = intent.id)

        is VehicleIntent.DismissAlert -> state.copy(
            vehicles = state.vehicles.map { v ->
                if (v.id == intent.id) v.copy(hasAlert = false, alertMessage = null) else v
            }
        )

        is VehicleIntent.ToggleCharging -> state.copy(
            vehicles = state.vehicles.map { v ->
                if (v.id == intent.id) v.copy(
                    status = if (v.status == ChargingStatus.Charging) ChargingStatus.Idle
                    else ChargingStatus.Charging
                ) else v
            }
        )

        is VehicleIntent.VehicleUpdated -> state.copy(
            vehicles = state.vehicles.map { v ->
                if (v.id == intent.vehicle.id) intent.vehicle else v
            },
            totalPowerKw = state.vehicles.sumOf {
                if (it.id == intent.vehicle.id) intent.vehicle.powerKw else it.powerKw
            }
        )
    }
}

// ─── Fake API ─────────────────────────────────────────────────────────────────

object FakeChargingApi {
    suspend fun fetchVehicles(): List<VehicleState> {
        delay(500)
        return listOf(
            VehicleState("v1", "Tesla Model 3",  0.73f, 11.0, ChargingStatus.Charging),
            VehicleState("v2", "Rivian R1T",     0.91f, 7.2,  ChargingStatus.Charging),
            VehicleState("v3", "Chevy Bolt",     0.45f, 0.0,  ChargingStatus.Fault,
                hasAlert = true, alertMessage = "Connector fault"),
            VehicleState("v4", "Ford F-150 Lightning", 1.0f, 0.0, ChargingStatus.Complete),
        )
    }

    /** Simulates real-time sensor updates */
    fun sensorStream(): Flow<VehicleState> = flow {
        val random = java.util.Random()
        while (true) {
            delay(2000)
            val id = "v${random.nextInt(4) + 1}"
            emit(VehicleState(id, "Vehicle $id",
                random.nextFloat().coerceIn(0f, 1f),
                random.nextDouble() * 22,
                ChargingStatus.Charging))
        }
    }
}

// ─── ViewModel ────────────────────────────────────────────────────────────────

class VehicleDashboardViewModel : ViewModel() {

    private val intentChannel = Channel<VehicleIntent>(Channel.UNLIMITED)

    private val _uiState = MutableStateFlow(VehicleUiState())
    val uiState: StateFlow<VehicleUiState> = _uiState.asStateFlow()

    init {
        // Process intents sequentially — no race conditions
        viewModelScope.launch {
            for (intent in intentChannel) {
                _uiState.update { VehicleReducer.reduce(it, intent) }
                handleSideEffect(intent)
            }
        }

        // Observe real-time sensor stream
        viewModelScope.launch {
            FakeChargingApi.sensorStream().collect { vehicle ->
                dispatch(VehicleIntent.VehicleUpdated(vehicle))
                // Auto-complete when fully charged
                if (vehicle.batteryPct >= 1.0f && vehicle.status == ChargingStatus.Charging) {
                    dispatch(VehicleIntent.ToggleCharging(vehicle.id))
                }
            }
        }

        dispatch(VehicleIntent.Refresh)
    }

    fun dispatch(intent: VehicleIntent) {
        intentChannel.trySend(intent)
    }

    private suspend fun handleSideEffect(intent: VehicleIntent) {
        when (intent) {
            is VehicleIntent.Refresh -> {
                try {
                    val vehicles = FakeChargingApi.fetchVehicles()
                    dispatch(VehicleIntent.LoadSuccess(vehicles))
                } catch (e: Exception) {
                    dispatch(VehicleIntent.LoadFailed(e.message ?: "Unknown error"))
                }
            }
            else -> Unit
        }
    }
}

// ─── Compose UI ───────────────────────────────────────────────────────────────

/*
@Composable
fun VehicleDashboard(viewModel: VehicleDashboardViewModel = viewModel()) {
    val state by viewModel.uiState.collectAsStateWithLifecycle()

    Scaffold { padding ->
        when {
            state.isLoading -> LoadingScreen()
            state.error != null -> ErrorScreen(state.error!!)
            state.vehicles.isEmpty() -> EmptyStateScreen()
            else -> VehicleGrid(state, viewModel::dispatch, Modifier.padding(padding))
        }
    }
}

@Composable
fun VehicleCard(vehicle: VehicleState, onIntent: (VehicleIntent) -> Unit) {
    val animatedBattery by animateFloatAsState(
        targetValue = vehicle.batteryPct,
        animationSpec = spring(dampingRatio = Spring.DampingRatioMediumBouncy),
        label = "battery_${vehicle.id}"
    )
    val statusColor = when (vehicle.status) {
        ChargingStatus.Charging -> Color.Green
        ChargingStatus.Complete -> Color.Blue
        ChargingStatus.Fault    -> Color.Red
        ChargingStatus.Idle     -> Color.Gray
    }

    Card(modifier = Modifier.fillMaxWidth().padding(8.dp)) {
        Column(modifier = Modifier.padding(16.dp)) {
            Row(horizontalArrangement = Arrangement.SpaceBetween, modifier = Modifier.fillMaxWidth()) {
                Text(vehicle.name, style = MaterialTheme.typography.titleMedium)
                Chip(label = vehicle.status.name, color = statusColor)
            }
            Spacer(modifier = Modifier.height(8.dp))
            LinearProgressIndicator(
                progress = animatedBattery,
                modifier = Modifier.fillMaxWidth().height(12.dp).clip(RoundedCornerShape(6.dp)),
                color = statusColor
            )
            Text("${(vehicle.batteryPct * 100).toInt()}% · ${vehicle.powerKw}kW",
                style = MaterialTheme.typography.bodySmall, color = Color.Gray)

            if (vehicle.hasAlert) {
                AlertBanner(vehicle.alertMessage ?: "Alert") {
                    onIntent(VehicleIntent.DismissAlert(vehicle.id))
                }
            }
        }
    }
}

@Composable
fun VehicleGrid(state: VehicleUiState, onIntent: (VehicleIntent) -> Unit, modifier: Modifier) {
    LazyVerticalGrid(
        columns = GridCells.Adaptive(180.dp),
        modifier = modifier,
        contentPadding = PaddingValues(8.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        if (state.criticalCount > 0) {
            item(span = { GridItemSpan(maxLineSpan) }) {
                CriticalAlertBanner(state.criticalCount)
            }
        }
        items(state.vehicles, key = { it.id }) { vehicle ->
            VehicleCard(vehicle, onIntent)
        }
    }
}
*/
```

---

## Alternative Approaches & Tradeoffs

| Approach | Pro | Con |
|---|---|---|
| MVI (this solution) | Predictable, testable, time-travel debug | More boilerplate than MVVM |
| MVVM + multiple `StateFlow` | Simple for small screens | State synchronization complexity at scale |
| Redux-style global store | Single source of truth for whole app | Overkill for single screen, harder to scope |
| `rememberSaveable` without ViewModel | Simple persistence | Lost on process death, hard to test |

---

## Unit Test Examples

```kotlin
class VehicleReducerTest {

    private val initialState = VehicleUiState(
        vehicles = listOf(
            VehicleState("v1", "Test Car", 0.5f, 11.0, ChargingStatus.Charging,
                hasAlert = true, alertMessage = "Fault")
        )
    )

    @Test
    fun `dismiss alert removes alert from vehicle`() {
        val newState = VehicleReducer.reduce(initialState, VehicleIntent.DismissAlert("v1"))
        val vehicle = newState.vehicles.first()
        assertFalse(vehicle.hasAlert)
        assertNull(vehicle.alertMessage)
    }

    @Test
    fun `toggle charging switches status`() {
        val newState = VehicleReducer.reduce(initialState, VehicleIntent.ToggleCharging("v1"))
        assertEquals(ChargingStatus.Idle, newState.vehicles.first().status)
    }

    @Test
    fun `refresh sets loading true`() {
        val newState = VehicleReducer.reduce(initialState, VehicleIntent.Refresh)
        assertTrue(newState.isLoading)
    }

    @Test
    fun `load success updates vehicles and clears loading`() {
        val loadingState = initialState.copy(isLoading = true)
        val newVehicles = listOf(VehicleState("v2", "New Car", 0.8f, 22.0, ChargingStatus.Complete))
        val newState = VehicleReducer.reduce(loadingState, VehicleIntent.LoadSuccess(newVehicles))

        assertFalse(newState.isLoading)
        assertEquals(1, newState.vehicles.size)
        assertEquals("v2", newState.vehicles.first().id)
    }

    @Test
    fun `total power is sum of all vehicles`() {
        val vehicles = listOf(
            VehicleState("v1", "A", 0.5f, 11.0, ChargingStatus.Charging),
            VehicleState("v2", "B", 0.8f, 7.2, ChargingStatus.Charging)
        )
        val newState = VehicleReducer.reduce(initialState, VehicleIntent.LoadSuccess(vehicles))
        assertEquals(18.2, newState.totalPowerKw, 0.001)
    }
}
```
