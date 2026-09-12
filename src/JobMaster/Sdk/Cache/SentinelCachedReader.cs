using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Cache;

/// <summary>
/// Reads a value through the in-memory cache, using a change sentinel to decide
/// when the cached entry is still fresh and falling back to a user-supplied factory
/// (with singleflight coalescing) on miss or staleness.
/// </summary>
/// <remarks>
/// <para>
/// The fetch flow on every call is:
/// <list type="number">
///   <item><description>Look up <c>cacheKey</c> in the cache.</description></item>
///   <item><description>If the entry exists and the sentinel reports no changes after the entry was
///   created (within <c>allowedDiscrepancy</c>), return the cached value — even if that value is
///   <see langword="null"/>. This preserves negative caching: a factory that returned <see langword="null"/>
///   keeps short-circuiting subsequent reads until the sentinel fires or the entry expires.</description></item>
///   <item><description>If the entry exists but the sentinel reports newer changes, evict it from the
///   cache so the next step does not see a stale singleflight winner.</description></item>
///   <item><description>Call <see cref="IJobMasterInMemoryCache.GetOrSet{T}"/> (or
///   <c>GetOrSetAsync</c>), which coalesces concurrent callers onto a single execution of
///   <paramref>factory</paramref> and stores the result.</description></item>
/// </list>
/// </para>
/// <para>
/// This type does NOT retry on transient database errors. Compose it with
/// <c>RetryDeadlockPolicy</c> at the call site when the underlying factory may surface
/// provider-specific deadlock exceptions:
/// <code>
/// retryDeadlockPolicy.Exec(() => sentinelCachedReader.GetOrFetch(...));
/// </code>
/// </para>
/// <para>
/// Factory exceptions propagate untouched. The singleflight semaphore inside
/// <c>GetOrSet</c> is released via <c>try/finally</c>, so a thrown factory does not poison
/// the lock or cache a failure value.
/// </para>
/// </remarks>
internal class SentinelCachedReader
{
    private readonly IJobMasterInMemoryCache cache;
    private readonly IMasterChangesSentinelService sentinelService;

    /// <summary>
    /// Creates a reader bound to a specific cache and sentinel service. Both collaborators
    /// are expected to be cluster-scoped — instances of this reader should not cross
    /// cluster boundaries.
    /// </summary>
    public SentinelCachedReader(
        IJobMasterInMemoryCache cache,
        IMasterChangesSentinelService sentinelService)
    {
        this.cache = cache;
        this.sentinelService = sentinelService;
    }

    /// <summary>
    /// Synchronous fetch. Returns the cached value when fresh; otherwise invokes
    /// <paramref name="factory"/> under singleflight and returns the produced value.
    /// </summary>
    /// <typeparam name="T">Cached value type. Reference and value types are both supported;
    /// for reference types, a <see langword="null"/> result is treated as a legitimate cached
    /// value (negative caching).</typeparam>
    /// <param name="cacheKey">Key the value is stored under in the in-memory cache.</param>
    /// <param name="sentinelKey">Sentinel key consulted to decide whether the cached entry
    /// is still fresh. Producers of changes to the underlying data must call
    /// <see cref="IMasterChangesSentinelService.NotifyChanges"/> with this same key.</param>
    /// <param name="factory">Invoked exactly once per cache miss/staleness across concurrent
    /// callers. May return <see langword="null"/>; the result is cached either way.</param>
    /// <param name="allowedDiscrepancy">Forwarded to
    /// <see cref="IMasterChangesSentinelService.HasChangesAfter"/>. Lets callers tolerate
    /// small clock skew or batched sentinel writes by treating recent changes as still
    /// "covered" by the existing entry. <see langword="null"/> means strict comparison.</param>
    /// <param name="valueFactoryLockDuration">How long a contending caller waits on the
    /// singleflight semaphore before falling through and invoking the factory itself. Forwarded
    /// to <see cref="IJobMasterInMemoryCache.GetOrSet{T}"/>.</param>
    /// <param name="durationToExpire">TTL applied to the cache entry. Sentinel invalidation
    /// is the primary freshness mechanism; this is the safety net.</param>
    public T GetOrFetch<T>(
        string cacheKey,
        string sentinelKey,
        Func<T> factory,
        TimeSpan? allowedDiscrepancy = null,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null)
    {
        var (found, freshValue) = TryReadFresh<T>(cacheKey, sentinelKey, allowedDiscrepancy);
        if (found)
        {
            return freshValue;
        }

        var cacheItem = cache.GetOrSet(
            cacheKey,
            factory,
            valueFactoryLockDuration: valueFactoryLockDuration,
            durationToExpire: durationToExpire);

        return cacheItem.Value!;
    }

    /// <summary>
    /// Asynchronous fetch. Mirrors <see cref="GetOrFetch{T}"/> but awaits the factory and
    /// the underlying <c>GetOrSetAsync</c>, so contention waits are non-blocking.
    /// </summary>
    /// <remarks>
    /// Prefer this overload from async call paths — it avoids the
    /// <c>Thread.Sleep</c>-on-contention behavior of the sync sibling and keeps the calling
    /// thread free during singleflight waits.
    /// </remarks>
    public async Task<T> GetOrFetchAsync<T>(
        string cacheKey,
        string sentinelKey,
        Func<Task<T>> factory,
        TimeSpan? allowedDiscrepancy = null,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null)
    {
        var (found, freshValue) = TryReadFresh<T>(cacheKey, sentinelKey, allowedDiscrepancy);
        if (found)
        {
            return freshValue;
        }

        var cacheItem = await cache.GetOrSetAsync(
            cacheKey,
            factory,
            valueFactoryLockDuration: valueFactoryLockDuration,
            durationToExpire: durationToExpire);

        return cacheItem.Value!;
    }

    /// <summary>
    /// Inspects the cache and sentinel to decide whether a fresh value is already available.
    /// Returns <c>(true, value)</c> when the cached entry is still fresh; returns
    /// <c>(false, default)</c> when the entry is missing or stale. Stale entries are removed
    /// here so the subsequent <c>GetOrSet</c> call does not short-circuit on the stale value.
    /// </summary>
    private (bool Found, T Value) TryReadFresh<T>(string cacheKey, string sentinelKey, TimeSpan? allowedDiscrepancy)
    {
        var cached = cache.Get<T>(cacheKey);
        if (cached is null)
        {
            return (false, default!);
        }

        if (!sentinelService.HasChangesAfter(sentinelKey, cached.CreatedAt, allowedDiscrepancy))
        {
            return (true, cached.Value!);
        }

        cache.Remove(cacheKey);
        return (false, default!);
    }
}
