using Microsoft.Extensions.Caching.Memory;

namespace JobMaster.Dashboard.AuthRetention;

internal sealed class InMemoryAuthRetentionService : IAuthRetentionService
{
    private readonly IMemoryCache cache;

    public InMemoryAuthRetentionService(IMemoryCache cache)
    {
        this.cache = cache;
    }

    public Task StoreAsync(string sessionId, string authKey, StoredAuth credentials, CancellationToken ct = default)
    {
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        cache.Set(CacheKey(sessionId, authKey), credentials, ttl);
        return Task.CompletedTask;
    }

    public Task<StoredAuth?> GetAsync(string sessionId, string authKey, CancellationToken ct = default)
    {
        cache.TryGetValue(CacheKey(sessionId, authKey), out StoredAuth? credentials);
        return Task.FromResult(credentials);
    }

    public Task RemoveAsync(string sessionId, string authKey, CancellationToken ct = default)
    {
        cache.Remove(CacheKey(sessionId, authKey));
        return Task.CompletedTask;
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
