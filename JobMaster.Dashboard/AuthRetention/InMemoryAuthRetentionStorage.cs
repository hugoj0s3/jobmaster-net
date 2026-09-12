using Microsoft.Extensions.Caching.Memory;

namespace JobMaster.Dashboard.AuthRetention;

internal sealed class InMemoryAuthRetentionStorage : IJobMasterAuthRetentionStorage
{
    private readonly IMemoryCache cache;

    public InMemoryAuthRetentionStorage(IMemoryCache cache)
    {
        this.cache = cache;
    }

    public Task StoreAsync(string sessionId, string authKey, RetainedCredential credentials)
    {
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        cache.Set(CacheKey(sessionId, authKey), credentials, ttl);
        return Task.CompletedTask;
    }

    public Task<RetainedCredential?> GetAsync(string sessionId, string authKey)
    {
        cache.TryGetValue(CacheKey(sessionId, authKey), out RetainedCredential? credentials);
        return Task.FromResult(credentials);
    }

    public Task RemoveAsync(string sessionId, string authKey)
    {
        cache.Remove(CacheKey(sessionId, authKey));
        return Task.CompletedTask;
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
