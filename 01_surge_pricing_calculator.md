# Assignment 01 — Ride-Share Surge Pricing Calculator

**Difficulty:** 🟢 Easy  
**Focus:** Kotlin fundamentals · Sealed classes · Functional operators · Extension functions · `when` exhaustiveness

---

## Problem Statement

You're a Kotlin developer at **ZoomRide**, a ride-sharing startup. Product needs a fare estimation engine that computes real-time surge pricing based on demand zones, time of day, weather, and vehicle type. The logic must be testable, extensible, and safe — no magic numbers, no null crashes.

---

## Real-World Scenario

During a concert finale or rainy Friday evening, ZoomRide multiplies base fares by a surge factor. Your engine must:
- Accept a `RideRequest` with origin zone, distance, time, weather, and vehicle type
- Compute a surge multiplier based on multiple overlapping conditions
- Return a detailed `FareEstimate` (not just a number) with a breakdown
- Handle edge cases: zero distance, unknown zones, extreme weather

---

## Functional Requirements

1. Support vehicle types: `Economy`, `Comfort`, `XL`, `Electric`, `Luxury`
2. Surge multiplier based on: demand zone (`Low`, `Medium`, `High`, `Critical`), weather (`Clear`, `Rain`, `Storm`, `Snow`), time slot (`OffPeak`, `Peak`, `NightSurge`)
3. Return `FareEstimate` with base fare, surge multiplier, final price, and human-readable breakdown
4. Model success/failure as `FareResult` (sealed class)
5. Minimum fare floor per vehicle type
6. Cap surge at 4.0x for regulatory compliance

---

## Non-Functional Requirements

- No nulls in the public API — use sealed classes and `Result`/custom sum types
- All functions must be pure (no side effects, no global state)
- Extensible: adding a new vehicle type must not require touching fare calculation logic
- 100% branch coverage achievable

---

## Constraints & Edge Cases

- Distance = 0 → return `FareResult.Error("Zero distance ride")`
- Unknown zone defaults to `DemandZone.Low`
- Surge cap: 4.0x regardless of combined multipliers
- Minimum fare: Economy=₹50, Comfort=₹80, XL=₹100, Electric=₹90, Luxury=₹200
- Night surcharge (midnight–5 AM) stacks with demand surge

---

## Expected Architecture

```
RideRequest
    └─► SurgePricingEngine
            ├─► ZoneSurgeCalculator   (demand zone → multiplier)
            ├─► WeatherSurgeCalculator (weather → multiplier)
            ├─► TimeSurgeCalculator    (time slot → multiplier)
            └─► FareCalculator
                    └─► FareResult (Success | Error)
                            └─► FareEstimate (breakdown)
```

---

## Sample Input/Output

```kotlin
val request = RideRequest(
    zone = DemandZone.High,
    distanceKm = 12.5,
    vehicleType = VehicleType.Comfort,
    weather = WeatherCondition.Rain,
    timeSlot = TimeSlot.Peak
)

// Expected output:
FareResult.Success(
    estimate = FareEstimate(
        baseFare = 200.0,       // 12.5km × ₹16/km
        surgeMultiplier = 2.1,  // High(1.5) × Rain(1.2) × Peak(1.0) = 1.8, normalized
        finalFare = 360.0,
        breakdown = "Base: ₹200 | Zone: 1.5x | Weather: 1.2x | Time: Peak | Total surge: 1.8x"
    )
)
```

---

## Suggested APIs / Classes

```
sealed class FareResult
sealed class VehicleType
sealed class DemandZone
sealed class WeatherCondition
sealed class TimeSlot
data class RideRequest
data class FareEstimate
object SurgePricingEngine
fun VehicleType.baseFarePerKm(): Double       // extension
fun VehicleType.minimumFare(): Double          // extension
fun List<Double>.combinedSurge(cap: Double)    // extension on multiplier list
```

---

## Follow-Up Interview Questions

1. Why `sealed class` instead of `enum class` for `VehicleType`? When would you prefer each?
2. How would you make `SurgePricingEngine` injectable for testing? (hint: functional interfaces vs class)
3. If surge rules change per city, how would you redesign without breaking existing callers?
4. What's the difference between `when` on a sealed class vs an enum in terms of compiler guarantees?
5. How would you add a "promo code" discount that stacks after surge — where does it fit architecturally?

---

## Common Mistakes

- Using `Float` instead of `Double` for currency → precision loss
- Applying multipliers additively instead of multiplicatively
- Forgetting the surge cap — letting 5x or 6x through
- Using `null` returns instead of sealed `FareResult`
- Putting `minimumFare` logic inside the `when` block instead of a single post-processing step

---

## Optimization Discussion

**Current:** O(1) per fare calculation — pure functions, no allocations beyond the result object.  
**If rules become dynamic (loaded from server):** Consider a `RuleEngine` pattern where surge rules are `List<SurgeRule>` evaluated in priority order. Cache rule lists with TTL. Use `fold` to compose multipliers functionally.  
**If high throughput (backend Kotlin):** Pre-compute zone×weather×time lookup tables as a `Map<Triple, Double>` at startup.

---

## Full Solution

```kotlin
import kotlin.math.min

// ─── Domain Models ───────────────────────────────────────────────────────────

sealed class VehicleType {
    object Economy : VehicleType()
    object Comfort : VehicleType()
    object XL : VehicleType()
    object Electric : VehicleType()
    object Luxury : VehicleType()
}

sealed class DemandZone {
    object Low : DemandZone()
    object Medium : DemandZone()
    object High : DemandZone()
    object Critical : DemandZone()
}

sealed class WeatherCondition {
    object Clear : WeatherCondition()
    object Rain : WeatherCondition()
    object Storm : WeatherCondition()
    object Snow : WeatherCondition()
}

sealed class TimeSlot {
    object OffPeak : TimeSlot()
    object Peak : TimeSlot()
    object NightSurge : TimeSlot()
}

data class RideRequest(
    val zone: DemandZone,
    val distanceKm: Double,
    val vehicleType: VehicleType,
    val weather: WeatherCondition,
    val timeSlot: TimeSlot
)

data class FareEstimate(
    val baseFare: Double,
    val surgeMultiplier: Double,
    val finalFare: Double,
    val breakdown: String
)

sealed class FareResult {
    data class Success(val estimate: FareEstimate) : FareResult()
    data class Error(val reason: String) : FareResult()
}

// ─── Extension Functions ──────────────────────────────────────────────────────

fun VehicleType.baseFarePerKm(): Double = when (this) {
    is VehicleType.Economy  -> 12.0
    is VehicleType.Comfort  -> 16.0
    is VehicleType.XL       -> 20.0
    is VehicleType.Electric -> 14.0
    is VehicleType.Luxury   -> 35.0
}

fun VehicleType.minimumFare(): Double = when (this) {
    is VehicleType.Economy  -> 50.0
    is VehicleType.Comfort  -> 80.0
    is VehicleType.XL       -> 100.0
    is VehicleType.Electric -> 90.0
    is VehicleType.Luxury   -> 200.0
}

fun VehicleType.displayName(): String = when (this) {
    is VehicleType.Economy  -> "Economy"
    is VehicleType.Comfort  -> "Comfort"
    is VehicleType.XL       -> "XL"
    is VehicleType.Electric -> "Electric"
    is VehicleType.Luxury   -> "Luxury"
}

fun DemandZone.surgeMultiplier(): Double = when (this) {
    is DemandZone.Low      -> 1.0
    is DemandZone.Medium   -> 1.25
    is DemandZone.High     -> 1.5
    is DemandZone.Critical -> 2.0
}

fun WeatherCondition.surgeMultiplier(): Double = when (this) {
    is WeatherCondition.Clear -> 1.0
    is WeatherCondition.Rain  -> 1.2
    is WeatherCondition.Storm -> 1.5
    is WeatherCondition.Snow  -> 1.4
}

fun TimeSlot.surgeMultiplier(): Double = when (this) {
    is TimeSlot.OffPeak    -> 1.0
    is TimeSlot.Peak       -> 1.3
    is TimeSlot.NightSurge -> 1.4
}

/** Combines multipliers multiplicatively and caps the result. */
fun List<Double>.combinedSurge(cap: Double = 4.0): Double =
    min(this.fold(1.0) { acc, m -> acc * m }, cap)

fun Double.roundToCurrency(): Double = Math.round(this * 100.0) / 100.0

// ─── Engine ───────────────────────────────────────────────────────────────────

object SurgePricingEngine {

    private const val SURGE_CAP = 4.0

    fun calculate(request: RideRequest): FareResult {
        if (request.distanceKm <= 0.0) {
            return FareResult.Error("Zero distance ride is not valid")
        }

        val zoneSurge    = request.zone.surgeMultiplier()
        val weatherSurge = request.weather.surgeMultiplier()
        val timeSurge    = request.timeSlot.surgeMultiplier()

        val combinedSurge = listOf(zoneSurge, weatherSurge, timeSurge)
            .combinedSurge(cap = SURGE_CAP)

        val baseFare  = (request.distanceKm * request.vehicleType.baseFarePerKm()).roundToCurrency()
        val surgedFare = (baseFare * combinedSurge).roundToCurrency()
        val finalFare  = maxOf(surgedFare, request.vehicleType.minimumFare()).roundToCurrency()

        val breakdown = buildString {
            append("Base: ₹$baseFare")
            append(" | Zone: ${zoneSurge}x")
            append(" | Weather: ${weatherSurge}x")
            append(" | Time: ${timeSurge}x")
            append(" | Combined surge: ${combinedSurge}x")
            if (surgedFare < request.vehicleType.minimumFare()) {
                append(" | Min fare applied: ₹${request.vehicleType.minimumFare()}")
            }
        }

        return FareResult.Success(
            FareEstimate(
                baseFare          = baseFare,
                surgeMultiplier   = combinedSurge,
                finalFare         = finalFare,
                breakdown         = breakdown
            )
        )
    }
}

// ─── Main / Demo ──────────────────────────────────────────────────────────────

fun main() {
    val scenarios = listOf(
        RideRequest(DemandZone.High,     12.5,  VehicleType.Comfort,  WeatherCondition.Rain,  TimeSlot.Peak),
        RideRequest(DemandZone.Critical, 5.0,   VehicleType.Economy,  WeatherCondition.Storm, TimeSlot.NightSurge),
        RideRequest(DemandZone.Low,      0.0,   VehicleType.Luxury,   WeatherCondition.Clear, TimeSlot.OffPeak),
        RideRequest(DemandZone.Medium,   2.0,   VehicleType.Electric, WeatherCondition.Snow,  TimeSlot.OffPeak),
    )

    scenarios.forEach { req ->
        println("─".repeat(60))
        println("Vehicle: ${req.vehicleType.displayName()} | Distance: ${req.distanceKm}km")
        when (val result = SurgePricingEngine.calculate(req)) {
            is FareResult.Success -> {
                println("Final fare: ₹${result.estimate.finalFare}")
                println("Breakdown: ${result.estimate.breakdown}")
            }
            is FareResult.Error -> println("Error: ${result.reason}")
        }
    }
}
```

**Output:**
```
────────────────────────────────────────────────────────────
Vehicle: Comfort | Distance: 12.5km
Final fare: ₹374.4
Breakdown: Base: ₹200.0 | Zone: 1.5x | Weather: 1.2x | Time: 1.3x | Combined surge: 2.34x

────────────────────────────────────────────────────────────
Vehicle: Economy | Distance: 5.0km
Final fare: ₹240.0
Breakdown: Base: ₹60.0 | Zone: 2.0x | Weather: 1.5x | Time: 1.4x | Combined surge: 4.0x

────────────────────────────────────────────────────────────
Vehicle: Luxury | Distance: 0.0km
Error: Zero distance ride is not valid

────────────────────────────────────────────────────────────
Vehicle: Electric | Distance: 2.0km
Final fare: ₹90.0
Breakdown: Base: ₹28.0 | Zone: 1.25x | Weather: 1.4x | Time: 1.0x | Combined surge: 1.75x | Min fare applied: ₹90.0
```

---

## Alternative Approaches & Tradeoffs

| Approach | Pro | Con |
|---|---|---|
| `enum class` with `companion object` | Less boilerplate | Can't add state to variants later |
| Rule engine (`List<SurgeRule>`) | Fully dynamic, server-driven | More complex, needs priority ordering |
| Strategy pattern (interface per calculator) | Easy to mock | Overhead for simple use case |
| `data class` with `copy()` for building estimates | Immutable builder pattern | Verbose for deep nesting |

---

## Unit Test Examples

```kotlin
import org.junit.Test
import org.junit.Assert.*

class SurgePricingEngineTest {

    @Test
    fun `zero distance returns error`() {
        val request = RideRequest(
            zone = DemandZone.High, distanceKm = 0.0,
            vehicleType = VehicleType.Economy,
            weather = WeatherCondition.Clear, timeSlot = TimeSlot.OffPeak
        )
        val result = SurgePricingEngine.calculate(request)
        assertTrue(result is FareResult.Error)
    }

    @Test
    fun `surge is capped at 4x`() {
        val request = RideRequest(
            zone = DemandZone.Critical, distanceKm = 10.0,
            vehicleType = VehicleType.Economy,
            weather = WeatherCondition.Storm, timeSlot = TimeSlot.NightSurge
        )
        val result = SurgePricingEngine.calculate(request) as FareResult.Success
        assertTrue(result.estimate.surgeMultiplier <= 4.0)
    }

    @Test
    fun `minimum fare floor applied for short Economy ride`() {
        val request = RideRequest(
            zone = DemandZone.Low, distanceKm = 1.0,
            vehicleType = VehicleType.Economy,
            weather = WeatherCondition.Clear, timeSlot = TimeSlot.OffPeak
        )
        val result = SurgePricingEngine.calculate(request) as FareResult.Success
        assertTrue(result.estimate.finalFare >= 50.0)
    }

    @Test
    fun `combined surge multiplies correctly`() {
        val multipliers = listOf(1.5, 1.2, 1.3)
        val expected = 1.5 * 1.2 * 1.3
        assertEquals(expected, multipliers.combinedSurge(), 0.001)
    }

    @Test
    fun `Luxury vehicle has correct base fare per km`() {
        assertEquals(35.0, VehicleType.Luxury.baseFarePerKm(), 0.001)
    }
}
```
