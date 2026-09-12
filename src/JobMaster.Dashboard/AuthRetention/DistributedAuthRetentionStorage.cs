using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace JobMaster.Dashboard.AuthRetention;

internal sealed class DistributedAuthRetentionStorage : IJobMasterAuthRetentionStorage
{
    private readonly IDistributedCache cache;

    public DistributedAuthRetentionStorage(IDistributedCache cache)
    {
        this.cache = cache;
    }

    public async Task StoreAsync(string sessionId, string authKey, RetainedCredential credentials)
    {
        var json = JsonSerializer.Serialize(credentials);
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = ttl };
        await cache.SetStringAsync(CacheKey(sessionId, authKey), json, options);
    }

    public async Task<RetainedCredential?> GetAsync(string sessionId, string authKey)
    {
        var json = await cache.GetStringAsync(CacheKey(sessionId, authKey));
        if (json is null) return null;
        return JsonSerializer.Deserialize<RetainedCredential>(json);
    }

    public async Task RemoveAsync(string sessionId, string authKey)
    {
        await cache.RemoveAsync(CacheKey(sessionId, authKey));
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
