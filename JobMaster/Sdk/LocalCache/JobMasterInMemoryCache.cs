using System.Collections.Concurrent;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.LocalCache;

namespace JobMaster.Sdk.LocalCache;

public class JobMasterInMemoryCache : IJobMasterInMemoryCache
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
           
        return cacheItem as JobMasterInMemoryCacheItem<T>;
    } 
    
    public void Set<T>(string key, T value, TimeSpan? durationToExpire = null)
    {
        durationToExpire ??= TimeSpan.FromHours(8);
        var utcNow = DateTime.UtcNow;
        var expiresAt = utcNow.Add(durationToExpire.Value);
        var cacheItem = new JobMasterInMemoryCacheItem<T>(utcNow, expiresAt, value);
        
        memoryCache[key] = cacheItem;
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