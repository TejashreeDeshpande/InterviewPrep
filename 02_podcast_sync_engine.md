# Assignment 02 — Podcast Episode Sync Engine

**Difficulty:** 🟢 Easy  
**Focus:** Coroutines · Flow · Channels · Structured concurrency · Progress tracking

---

## Problem Statement

You're building **PodFlow**, a podcast app. Users subscribe to shows and expect episodes to be automatically downloaded in the background — with live progress, pause/resume, and the ability to cancel individual downloads without killing others. Your sync engine must handle multiple concurrent downloads gracefully.

---

## Real-World Scenario

When a user opens the app offline, they need episodes ready. The sync engine should:
- Queue up to N episodes for download concurrently
- Emit real-time download progress as a `Flow`
- Handle transient failures with retry (exponential back-off)
- Allow cancellation of individual episodes or the whole queue
- Respect low-power mode (reduce concurrency to 1)

---

## Functional Requirements

1. `PodcastSyncEngine.sync(episodes: List<Episode>)` — starts the pipeline
2. `downloadProgress: Flow<DownloadEvent>` — hot stream of progress events
3. Max 3 concurrent downloads (configurable)
4. Each download emits: `Started`, `Progress(percent)`, `Completed`, `Failed(reason)`, `Cancelled`
5. Retry failed downloads up to 2 times with exponential back-off (1s, 2s)
6. Individual episode cancellation via `cancel(episodeId: String)`

---

## Non-Functional Requirements

- Downloads run on `Dispatchers.IO`
- Progress updates debounced to max 10/sec (no UI flooding)
- Engine scope tied to lifecycle — cancelling scope cancels all downloads
- Memory-efficient: no buffering of downloaded bytes in RAM

---

## Constraints & Edge Cases

- Empty episode list → `Flow` completes immediately
- Episode already downloaded → emit `Completed` immediately, skip download
- Network drop mid-download → emit `Failed`, trigger retry
- All retries exhausted → emit `Failed(permanent = true)`
- Concurrency = 0 is invalid → throw `IllegalArgumentException` at construction

---

## Expected Architecture

```
PodcastSyncEngine
    ├── episodeChannel: Channel<Episode>          (work queue)
    ├── _events: MutableSharedFlow<DownloadEvent> (hot event bus)
    ├── downloadProgress: SharedFlow<DownloadEvent> (public)
    └── workers: List<Job>                        (N coroutine workers)

DownloadEvent (sealed class)
    ├── Started(episodeId)
    ├── Progress(episodeId, percent: Int)
    ├── Completed(episodeId, filePath: String)
    ├── Failed(episodeId, reason: String, permanent: Boolean)
    └── Cancelled(episodeId)
```

---

## Sample Input/Output

```kotlin
val engine = PodcastSyncEngine(maxConcurrent = 2)
engine.downloadProgress.collect { event ->
    when (event) {
        is DownloadEvent.Progress   -> println("${event.episodeId}: ${event.percent}%")
        is DownloadEvent.Completed  -> println("${event.episodeId}: Done ✓")
        is DownloadEvent.Failed     -> println("${event.episodeId}: Failed — ${event.reason}")
        else -> {}
    }
}

engine.sync(listOf(ep1, ep2, ep3, ep4))
// ep1 & ep2 download concurrently, ep3 & ep4 queue
// ep1: 10%, 20%, ... 100% → Completed
// ep2: Failed (retry 1) → Failed (retry 2) → Failed(permanent=true)
```

---

## Suggested APIs / Classes

```
Channel<Episode>(capacity = Channel.UNLIMITED)
MutableSharedFlow<DownloadEvent>(replay = 0, extraBufferCapacity = 64)
coroutineScope { repeat(maxConcurrent) { launch { worker() } } }
kotlinx.coroutines.delay()         // back-off
flow { }.retryWhen { cause, attempt -> }
supervisorScope { }                // isolate worker failures
Job.cancel(CancellationException)
```

---

## Follow-Up Interview Questions

1. Why use `Channel` as a work queue instead of launching one coroutine per episode?
2. What's the difference between `SharedFlow` and `StateFlow`? Why is `SharedFlow` correct here?
3. When would you use `supervisorScope` vs `coroutineScope`? What happens if a worker crashes?
4. How does structured concurrency prevent download leaks when the user leaves the screen?
5. How would you add a pause/resume feature — what synchronization primitives would you use?
6. `retryWhen` vs manually looping with try/catch — tradeoffs?

---

## Common Mistakes

- Launching one coroutine per episode (unbounded parallelism — OOM risk)
- Using `StateFlow` for events — subscribers miss events emitted before they collect
- Catching `CancellationException` and swallowing it — breaks structured cancellation
- Not using `supervisorScope` — one failed download kills all others
- Emitting progress from `Dispatchers.IO` directly to UI without switching context

---

## Optimization Discussion

**Debouncing progress:** Use `conflate()` or a ticker channel to limit UI updates to 10/sec.  
**Memory:** Stream bytes directly to disk via `OutputStream` — never load the full file into a `ByteArray`.  
**Priority queue:** Replace `Channel` with a `PriorityChannel` (custom) that always pulls the most-recently-requested episode next for better UX.  
**Battery:** In low-power mode, reduce `maxConcurrent` to 1 and pause downloads on cellular — expose a `setLowPowerMode(Boolean)` that dynamically adjusts the semaphore.

---

## Full Solution

```kotlin
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.math.pow

// ─── Domain Models ───────────────────────────────────────────────────────────

data class Episode(
    val id: String,
    val title: String,
    val audioUrl: String,
    val isAlreadyDownloaded: Boolean = false
)

sealed class DownloadEvent {
    abstract val episodeId: String
    data class Started(override val episodeId: String) : DownloadEvent()
    data class Progress(override val episodeId: String, val percent: Int) : DownloadEvent()
    data class Completed(override val episodeId: String, val filePath: String) : DownloadEvent()
    data class Failed(override val episodeId: String, val reason: String, val permanent: Boolean = false) : DownloadEvent()
    data class Cancelled(override val episodeId: String) : DownloadEvent()
}

// ─── Fake Downloader (simulates network) ─────────────────────────────────────

interface EpisodeDownloader {
    /** Calls [onProgress] with 0..100, returns the file path or throws. */
    suspend fun download(episode: Episode, onProgress: (Int) -> Unit): String
}

class FakeEpisodeDownloader(
    private val failIds: Set<String> = emptySet()
) : EpisodeDownloader {
    override suspend fun download(episode: Episode, onProgress: (Int) -> Unit): String {
        if (episode.id in failIds) throw Exception("Simulated network error")
        for (percent in 0..100 step 10) {
            delay(50)
            onProgress(percent)
        }
        return "/podcasts/${episode.id}.mp3"
    }
}

// ─── Engine ──────────────────────────────────────────────────────────────────

class PodcastSyncEngine(
    private val maxConcurrent: Int = 3,
    private val maxRetries: Int = 2,
    private val downloader: EpisodeDownloader = FakeEpisodeDownloader(),
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
) {
    init {
        require(maxConcurrent > 0) { "maxConcurrent must be > 0" }
    }

    private val _events = MutableSharedFlow<DownloadEvent>(
        replay = 0,
        extraBufferCapacity = 128
    )
    val downloadProgress: SharedFlow<DownloadEvent> = _events.asSharedFlow()

    private val episodeChannel = Channel<Episode>(capacity = Channel.UNLIMITED)

    // Map of episodeId → worker Job for individual cancellation
    private val activeJobs = mutableMapOf<String, Job>()

    fun sync(episodes: List<Episode>) {
        // Launch N workers
        repeat(maxConcurrent) {
            scope.launch { worker() }
        }
        // Enqueue all episodes
        episodes.forEach { episode ->
            episodeChannel.trySend(episode)
        }
        // Close channel so workers finish after draining
        episodeChannel.close()
    }

    fun cancel(episodeId: String) {
        activeJobs[episodeId]?.cancel(CancellationException("User cancelled $episodeId"))
    }

    fun shutdown() {
        scope.cancel()
    }

    private suspend fun worker() {
        for (episode in episodeChannel) {
            val job = scope.launch { processEpisode(episode) }
            activeJobs[episode.id] = job
            job.join()
            activeJobs.remove(episode.id)
        }
    }

    private suspend fun processEpisode(episode: Episode) {
        if (episode.isAlreadyDownloaded) {
            _events.emit(DownloadEvent.Completed(episode.id, "/podcasts/${episode.id}.mp3"))
            return
        }

        _events.emit(DownloadEvent.Started(episode.id))

        var attempt = 0
        while (attempt <= maxRetries) {
            try {
                val path = downloader.download(episode) { percent ->
                    scope.launch {
                        _events.emit(DownloadEvent.Progress(episode.id, percent))
                    }
                }
                _events.emit(DownloadEvent.Completed(episode.id, path))
                return
            } catch (e: CancellationException) {
                _events.emit(DownloadEvent.Cancelled(episode.id))
                throw e  // must re-throw CancellationException
            } catch (e: Exception) {
                attempt++
                if (attempt > maxRetries) {
                    _events.emit(DownloadEvent.Failed(episode.id, e.message ?: "Unknown", permanent = true))
                    return
                }
                val backoffMs = (1000L * 2.0.pow(attempt - 1)).toLong()
                _events.emit(DownloadEvent.Failed(episode.id, "Retry $attempt after ${backoffMs}ms"))
                delay(backoffMs)
            }
        }
    }
}

// ─── Main / Demo ──────────────────────────────────────────────────────────────

fun main() = runBlocking {
    val engine = PodcastSyncEngine(
        maxConcurrent = 2,
        downloader = FakeEpisodeDownloader(failIds = setOf("ep3"))
    )

    val collectorJob = launch {
        engine.downloadProgress.collect { event ->
            when (event) {
                is DownloadEvent.Started   -> println("[${event.episodeId}] Started")
                is DownloadEvent.Progress  -> print("\r[${event.episodeId}] ${event.percent}%   ")
                is DownloadEvent.Completed -> println("\n[${event.episodeId}] ✓ Saved to ${event.filePath}")
                is DownloadEvent.Failed    -> println("\n[${event.episodeId}] ✗ ${event.reason}")
                is DownloadEvent.Cancelled -> println("\n[${event.episodeId}] Cancelled")
            }
        }
    }

    val episodes = listOf(
        Episode("ep1", "Kotlin Coroutines Deep Dive", "https://example.com/ep1.mp3"),
        Episode("ep2", "Jetpack Compose Internals",  "https://example.com/ep2.mp3"),
        Episode("ep3", "Flow Operators",              "https://example.com/ep3.mp3"),
        Episode("ep4", "Room Best Practices",         "https://example.com/ep4.mp3", isAlreadyDownloaded = true),
    )

    engine.sync(episodes)
    delay(3000)

    collectorJob.cancel()
    engine.shutdown()
}
```

---

## Alternative Approaches & Tradeoffs

| Approach | Pro | Con |
|---|---|---|
| `Channel` + N workers | Bounded parallelism, back-pressure | Channel must be closed explicitly |
| `flatMapMerge(concurrency = N)` on a `Flow<Episode>` | Elegant, declarative | Less control over individual cancellation |
| `Semaphore(N)` + launch per episode | Easy individual cancellation | Spawns unbounded coroutines upfront |
| `WorkManager` | Survives process death | Overkill for in-session downloads, slower startup |

---

## Unit Test Examples

```kotlin
import app.cash.turbine.test
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.Assert.*

class PodcastSyncEngineTest {

    @Test
    fun `already downloaded episode emits Completed immediately`() = runTest {
        val engine = PodcastSyncEngine(scope = CoroutineScope(this.coroutineContext))
        val ep = Episode("ep1", "Title", "url", isAlreadyDownloaded = true)

        engine.downloadProgress.test {
            engine.sync(listOf(ep))
            val event = awaitItem()
            assertTrue(event is DownloadEvent.Completed)
            assertEquals("ep1", event.episodeId)
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `failed episode retries and emits permanent failure`() = runTest {
        val downloader = FakeEpisodeDownloader(failIds = setOf("ep1"))
        val engine = PodcastSyncEngine(
            maxRetries = 2,
            downloader = downloader,
            scope = CoroutineScope(this.coroutineContext)
        )
        val ep = Episode("ep1", "Title", "url")

        engine.downloadProgress.test {
            engine.sync(listOf(ep))
            val events = mutableListOf<DownloadEvent>()
            repeat(4) { events.add(awaitItem()) } // Started + 2 Failed + 1 permanent Failed
            assertTrue(events.last() is DownloadEvent.Failed)
            assertTrue((events.last() as DownloadEvent.Failed).permanent)
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `engine respects max concurrency`() = runTest {
        var peakConcurrent = 0
        var current = 0
        val downloader = object : EpisodeDownloader {
            override suspend fun download(episode: Episode, onProgress: (Int) -> Unit): String {
                current++
                peakConcurrent = maxOf(peakConcurrent, current)
                delay(100)
                current--
                return "/path"
            }
        }
        val engine = PodcastSyncEngine(maxConcurrent = 2, downloader = downloader,
            scope = CoroutineScope(this.coroutineContext))
        val episodes = List(6) { Episode("ep$it", "Title $it", "url$it") }
        engine.sync(episodes)
        delay(1000)
        assertTrue(peakConcurrent <= 2)
    }
}
```
