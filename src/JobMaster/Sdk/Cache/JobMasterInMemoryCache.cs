using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Cache;

internal class JobMasterInMemoryCache : IJobMasterInMemoryCache
{
    private static readonly ConcurrentDictionary<string, JobMasterInMemoryCacheItem> memoryCache = new();
    private static readonly Timer CleanupTimer;
    
    static JobMasterInMemoryCache()
    {
        // Run cleanup every 5 minutes
        CleanupTimer = new Timer(CleanupExpiredItems, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    public JobMasterInMemoryCache()
    {
    }

    public JobMasterInMemoryCacheItem<T>? Get<T>(string key)
    {
        var cacheItem = memoryCache.GetOrDefault(key);
        if (cacheItem is null) return null;
        if (cacheItem.ExpiresAt < DateTime.UtcNow)
        {
            memoryCache.TryRemove(key, out _);
            return null;
        }

        var typedItem = cacheItem as JobMasterInMemoryCacheItem<T>;
        if (typedItem is null) return null;

        var clone = typedItem.Value.DeepCloneViaGenericEntry();
        return new JobMasterInMemoryCacheItem<T>(typedItem.CreatedAt, typedItem.ExpiresAt, clone);
    }

    public JobMasterInMemoryCacheItem<T> Set<T>(string key, T value, TimeSpan? durationToExpire = null)
    {
        durationToExpire ??= JobMasterConstants.DefaultCacheEntryExpiry;
        var utcNow = DateTime.UtcNow;
        var expiresAt = utcNow.Add(durationToExpire.Value);

        var snapshot = value.DeepCloneViaGenericEntry();
        var cacheItem = new JobMasterInMemoryCacheItem<T>(utcNow, expiresAt, snapshot);

        memoryCache[key] = cacheItem;
        
        return cacheItem;
    }

    private static ConcurrentDictionary<string, SemaphoreSlim> valueFactoryLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
    public JobMasterInMemoryCacheItem<T> GetOrSet<T>(
        string key,
        Func<T> valueFactory,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null)
    {
        valueFactoryLockDuration ??= TimeSpan.FromSeconds(1);
        var cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var valueFactoryLock = valueFactoryLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

        var acquired = valueFactoryLock.Wait(valueFactoryLockDuration.Value);
        if (acquired)
        {
            try
            {
                cacheItem = Get<T>(key);
                if (cacheItem is not null) return cacheItem;

                return Set(key, valueFactory(), durationToExpire);
            }
            finally
            {
                valueFactoryLock.Release();
            }
        }

        // Lock timed out — the lock holder will populate the cache when it finishes.
        // Do NOT call Set here: writing without the lock can overwrite the lock holder's
        // result with a value read from an older DB snapshot, bypassing sentinel detection.
        cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var freshValue = valueFactory();
        cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var utcNow = DateTime.UtcNow;
        return new JobMasterInMemoryCacheItem<T>(
            utcNow,
            utcNow.Add(durationToExpire ?? JobMasterConstants.DefaultCacheEntryExpiry),
            freshValue);
    }

    public async Task<JobMasterInMemoryCacheItem<T>> GetOrSetAsync<T>(
        string key,
        Func<Task<T>> valueFactory,
        TimeSpan? valueFactoryLockDuration = null,
        TimeSpan? durationToExpire = null)
    {
        valueFactoryLockDuration ??= TimeSpan.FromSeconds(1);
        var cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var valueFactoryLock = valueFactoryLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

        var acquired = await valueFactoryLock.WaitAsync(valueFactoryLockDuration.Value);
        if (acquired)
        {
            try
            {
                cacheItem = Get<T>(key);
                if (cacheItem is not null) return cacheItem;
                return Set(key, await valueFactory(), durationToExpire);
            }
            finally
            {
                valueFactoryLock.Release();
            }
        }

        // Lock timed out — the lock holder will populate the cache when it finishes.
        // Do NOT call Set here: writing without the lock can overwrite the lock holder's
        // result with a value read from an older DB snapshot, bypassing sentinel detection.
        cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var freshValue = await valueFactory();
        cacheItem = Get<T>(key);
        if (cacheItem is not null) return cacheItem;

        var utcNow = DateTime.UtcNow;
        return new JobMasterInMemoryCacheItem<T>(
            utcNow,
            utcNow.Add(durationToExpire ?? JobMasterConstants.DefaultCacheEntryExpiry),
            freshValue);
    }
    
    public bool Exists(string key)
    {
        return memoryCache.TryGetValue(key, out _);
    }

    public void Remove(string key)
    {
        memoryCache.TryRemove(key, out _);
    }
    
    private static void CleanupExpiredItems(object? state)
    {
        var now = DateTime.UtcNow;
        var expiredKeys = new List<string>();
        
        // Find all expired keys
        foreach (var kvp in memoryCache)
        {
            if (kvp.Value.ExpiresAt < now)
            {
                expiredKeys.Add(kvp.Key);
            }
        }
        
        // Remove expired items
        foreach (var key in expiredKeys)
        {
            memoryCache.TryRemove(key, out _);
        }
    }
}