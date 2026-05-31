using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace JobMaster.Dashboard.AuthRetention;

internal sealed class DistributedAuthRetentionService : IAuthRetentionService
{
    private readonly IDistributedCache cache;

    public DistributedAuthRetentionService(IDistributedCache cache)
    {
        this.cache = cache;
    }

    public async Task StoreAsync(string sessionId, string authKey, StoredAuth credentials, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(credentials);
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = ttl };
        await cache.SetStringAsync(CacheKey(sessionId, authKey), json, options, ct);
    }

    public async Task<StoredAuth?> GetAsync(string sessionId, string authKey, CancellationToken ct = default)
    {
        var json = await cache.GetStringAsync(CacheKey(sessionId, authKey), ct);
        if (json is null) return null;
        return JsonSerializer.Deserialize<StoredAuth>(json);
    }

    public async Task RemoveAsync(string sessionId, string authKey, CancellationToken ct = default)
    {
        await cache.RemoveAsync(CacheKey(sessionId, authKey), ct);
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
