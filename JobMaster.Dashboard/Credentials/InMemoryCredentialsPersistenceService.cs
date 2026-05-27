using Microsoft.Extensions.Caching.Memory;

namespace JobMaster.Dashboard.Credentials;

internal sealed class InMemoryCredentialsPersistenceService : ICredentialsPersistenceService
{
    private readonly IMemoryCache cache;

    public InMemoryCredentialsPersistenceService(IMemoryCache cache)
    {
        this.cache = cache;
    }

    public Task StoreAsync(string sessionId, string credentialKey, StoredCredentials credentials, CancellationToken ct = default)
    {
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        cache.Set(CacheKey(sessionId, credentialKey), credentials, ttl);
        return Task.CompletedTask;
    }

    public Task<StoredCredentials?> GetAsync(string sessionId, string credentialKey, CancellationToken ct = default)
    {
        cache.TryGetValue(CacheKey(sessionId, credentialKey), out StoredCredentials? credentials);
        return Task.FromResult(credentials);
    }

    public Task RemoveAsync(string sessionId, string credentialKey, CancellationToken ct = default)
    {
        cache.Remove(CacheKey(sessionId, credentialKey));
        return Task.CompletedTask;
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
