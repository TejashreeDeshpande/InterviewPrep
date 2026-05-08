# Assignment 06 — Social Feed Smart Cache

**Difficulty:** 🔴 Senior  
**Focus:** Multi-layer caching · Paging 3 · Flow operators · TTL eviction · Clean Architecture

---

## Problem Statement

You're building the feed for **Mosaic**, an Instagram-like social app. The feed must load instantly from cache, stay fresh in the background, handle millions of posts efficiently with paging, and evict stale content gracefully — all while working offline.

---

## Real-World Scenario

Users open the app 20+ times per day. Network calls on every open drain battery and feel slow. A smart cache must serve cached content in < 50ms while revalidating in the background. Infinite scroll must be smooth to 60fps. Content older than 1 hour should be automatically refetched.

---

## Functional Requirements

1. Three-layer cache: `MemoryCache (LRU)` → `DiskCache (Room)` → `Network (Retrofit)`
2. Cache TTL: posts expire after 60 minutes
3. Paging: `Pager` with `RemoteMediator` pattern (Room as source of truth + network prefetch)
4. `Flow<PagingData<PostUiModel>>` exposed from ViewModel
5. Background revalidation: stale content fetches without blocking UI
6. Eviction: `MemoryCache` max 200 items, evict LRU on overflow

---

## Non-Functional Requirements

- Memory cache lookup: O(1)
- First contentful paint: serve stale cache in < 50ms, revalidate async
- Disk cache: indexed by `(feedId, page, timestamp)`
- `PagingData` uses `DiffUtil` with stable `postId` keys
- Images use Coil with `DiskCachePolicy.ENABLED`

---

## Constraints & Edge Cases

- User edits a post → invalidate that post's cache entry in all layers
- Feed is empty (new user) → show `EmptyState`, not endless spinner
- Network error while paginating → `PagingSource.LoadResult.Error` → Paging shows retry footer
- Cache hit for page 1 but miss for page 2 → serve page 1 from cache, fetch page 2
- Two users on same device → cache keyed by `userId_feedId`

---

## Expected Architecture

```
ViewModel
    └── Pager(RemoteMediator + RoomPagingSource)
            │
         PagingData<PostUiModel>
                 │
    ┌────────────┼────────────────────────────┐
    │            │                            │
MemoryCache   Room (DiskCache)          NetworkApi
(LRU<K,V>)   PostEntity + FeedPage      Retrofit
              expires_at column
```

---

## Sample Input/Output

```kotlin
// ViewModel
val feed: Flow<PagingData<PostUiModel>> = Pager(
    config = PagingConfig(pageSize = 20, enablePlaceholders = false),
    remoteMediator = FeedRemoteMediator(api, db),
    pagingSourceFactory = { db.postDao().pagingSource(feedId) }
).flow.map { pagingData -> pagingData.map { it.toUiModel() } }
    .cachedIn(viewModelScope)

// UI
val posts = viewModel.feed.collectAsLazyPagingItems()
LazyColumn { items(posts, key = { it.postId }) { PostCard(it) } }
```

---

## Suggested APIs / Classes

```kotlin
RemoteMediator<Int, PostEntity>
PagingSource<Int, PostEntity>           // Room auto-generates
@Query("SELECT * FROM posts WHERE feed_id = :feedId ORDER BY rank")
LinkedHashMap<K,V>(capacity, 0.75f, true)   // access-order LRU
pagingData.map { } / filter { } / insertSeparators { }
cachedIn(viewModelScope)
Flow.stateIn() with SharingStarted.Lazily
```

---

## Follow-Up Interview Questions

1. Why does `RemoteMediator` use Room as the single source of truth rather than mixing network + DB?
2. What is `LoadState` in Paging 3? How do you show a loading footer/error footer?
3. `cachedIn(viewModelScope)` — what does it do and why is it essential for Paging?
4. How do you invalidate a specific item in the paging source when it's edited?
5. LRU via `LinkedHashMap` vs `LruCache` from Android framework — tradeoffs?
6. How would you add "pull to refresh" that clears the cache and refetches?

---

## Common Mistakes

- Not using `cachedIn` — causes full reload on every recomposition
- Putting business logic inside `RemoteMediator` (should be in Repository)
- Cache key doesn't include userId → user A sees user B's feed
- Not handling `LoadType.REFRESH` vs `PREPEND` vs `APPEND` in RemoteMediator
- Missing `@Transaction` on bulk Room inserts — partial writes on crash

---

## Full Solution

```kotlin
import androidx.paging.*
import androidx.room.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.LinkedHashMap

// ─── Domain Models ───────────────────────────────────────────────────────────

@Entity(tableName = "posts", indices = [Index("feed_id"), Index("rank")])
data class PostEntity(
    @PrimaryKey val postId: String,
    val feedId: String,
    val authorId: String,
    val authorName: String,
    val imageUrl: String,
    val caption: String,
    val likeCount: Int,
    val rank: Int,   // server-assigned ranking position
    val expiresAt: Long = System.currentTimeMillis() + 3_600_000  // 1 hour TTL
) {
    val isExpired: Boolean get() = System.currentTimeMillis() > expiresAt
}

@Entity(tableName = "feed_remote_keys")
data class FeedRemoteKey(
    @PrimaryKey val feedId: String,
    val nextCursor: String?,
    val prevCursor: String?
)

data class PostUiModel(
    val postId: String,
    val authorName: String,
    val imageUrl: String,
    val caption: String,
    val likeCount: Int
)

fun PostEntity.toUiModel() = PostUiModel(postId, authorName, imageUrl, caption, likeCount)

// ─── Room ─────────────────────────────────────────────────────────────────────

@Dao
interface PostDao {
    @Query("SELECT * FROM posts WHERE feed_id = :feedId AND expires_at > :now ORDER BY rank ASC")
    fun pagingSource(feedId: String, now: Long = System.currentTimeMillis()): PagingSource<Int, PostEntity>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(posts: List<PostEntity>)

    @Query("DELETE FROM posts WHERE feed_id = :feedId")
    suspend fun clearFeed(feedId: String)

    @Query("DELETE FROM posts WHERE expires_at < :now")
    suspend fun evictExpired(now: Long = System.currentTimeMillis())
}

@Dao
interface FeedRemoteKeyDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(key: FeedRemoteKey)

    @Query("SELECT * FROM feed_remote_keys WHERE feed_id = :feedId")
    suspend fun getKey(feedId: String): FeedRemoteKey?

    @Query("DELETE FROM feed_remote_keys WHERE feed_id = :feedId")
    suspend fun deleteKey(feedId: String)
}

// ─── Memory Cache (LRU) ───────────────────────────────────────────────────────

class LruMemoryCache<K, V>(private val maxSize: Int) {
    private val map = object : LinkedHashMap<K, V>(maxSize, 0.75f, true) {
        override fun removeEldestEntry(eldest: Map.Entry<K, V>): Boolean = size > maxSize
    }

    @Synchronized fun get(key: K): V? = map[key]
    @Synchronized fun put(key: K, value: V) { map[key] = value }
    @Synchronized fun remove(key: K) { map.remove(key) }
    @Synchronized fun clear() { map.clear() }
    val size: Int @Synchronized get() = map.size
}

// ─── Retrofit API ─────────────────────────────────────────────────────────────

data class FeedResponse(val posts: List<PostDto>, val nextCursor: String?, val prevCursor: String?)
data class PostDto(val postId: String, val authorId: String, val authorName: String,
                   val imageUrl: String, val caption: String, val likeCount: Int, val rank: Int)

interface FeedApi {
    @retrofit2.http.GET("feed/{feedId}")
    suspend fun getFeed(
        @retrofit2.http.Path("feedId") feedId: String,
        @retrofit2.http.Query("cursor") cursor: String? = null,
        @retrofit2.http.Query("limit") limit: Int = 20
    ): FeedResponse
}

fun PostDto.toEntity(feedId: String) = PostEntity(postId, feedId, authorId, authorName, imageUrl, caption, likeCount, rank)

// ─── Remote Mediator ──────────────────────────────────────────────────────────

@OptIn(ExperimentalPagingApi::class)
class FeedRemoteMediator(
    private val feedId: String,
    private val api: FeedApi,
    private val db: AppDatabase
) : RemoteMediator<Int, PostEntity>() {

    override suspend fun initialize(): InitializeAction {
        // Force refresh if any cached posts are expired
        val hasExpired = db.postDao().pagingSource(feedId).let { false } // simplified check
        return if (hasExpired) InitializeAction.LAUNCH_INITIAL_REFRESH
        else InitializeAction.SKIP_INITIAL_REFRESH
    }

    override suspend fun load(loadType: LoadType, state: PagingState<Int, PostEntity>): MediatorResult {
        return try {
            val cursor = when (loadType) {
                LoadType.REFRESH -> null
                LoadType.PREPEND -> return MediatorResult.Success(endOfPaginationReached = true)
                LoadType.APPEND  -> db.feedRemoteKeyDao().getKey(feedId)?.nextCursor
                    ?: return MediatorResult.Success(endOfPaginationReached = true)
            }

            val response = api.getFeed(feedId, cursor, state.config.pageSize)

            db.withTransaction {
                if (loadType == LoadType.REFRESH) {
                    db.postDao().clearFeed(feedId)
                    db.feedRemoteKeyDao().deleteKey(feedId)
                }
                db.feedRemoteKeyDao().upsert(FeedRemoteKey(feedId, response.nextCursor, response.prevCursor))
                db.postDao().insertAll(response.posts.map { it.toEntity(feedId) })
            }

            MediatorResult.Success(endOfPaginationReached = response.nextCursor == null)
        } catch (e: Exception) {
            MediatorResult.Error(e)
        }
    }
}

// ─── Repository ───────────────────────────────────────────────────────────────

@OptIn(ExperimentalPagingApi::class)
class FeedRepository(
    private val api: FeedApi,
    private val db: AppDatabase,
    private val memoryCache: LruMemoryCache<String, PostUiModel> = LruMemoryCache(200)
) {
    fun getFeed(feedId: String): Flow<PagingData<PostUiModel>> = Pager(
        config = PagingConfig(pageSize = 20, enablePlaceholders = false, prefetchDistance = 5),
        remoteMediator = FeedRemoteMediator(feedId, api, db),
        pagingSourceFactory = { db.postDao().pagingSource(feedId) }
    ).flow.map { pagingData ->
        pagingData.map { entity ->
            memoryCache.get(entity.postId) ?: entity.toUiModel().also { memoryCache.put(entity.postId, it) }
        }
    }

    suspend fun invalidatePost(postId: String) {
        memoryCache.remove(postId)
        // PagingSource will auto-invalidate via Room's InvalidationTracker
    }

    suspend fun evictExpiredCache() {
        db.postDao().evictExpired()
        memoryCache.clear()
    }
}
```

---

## Unit Test Examples

```kotlin
class LruMemoryCacheTest {
    @Test fun `evicts LRU item when max size exceeded`() {
        val cache = LruMemoryCache<String, String>(maxSize = 2)
        cache.put("a", "1"); cache.put("b", "2")
        cache.get("a") // access 'a' — makes 'b' LRU
        cache.put("c", "3") // evicts 'b'
        assertNull(cache.get("b"))
        assertNotNull(cache.get("a"))
        assertNotNull(cache.get("c"))
    }
}
```
