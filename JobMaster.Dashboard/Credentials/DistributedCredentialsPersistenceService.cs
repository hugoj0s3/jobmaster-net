using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace JobMaster.Dashboard.Credentials;

internal sealed class DistributedCredentialsPersistenceService : ICredentialsPersistenceService
{
    private readonly IDistributedCache cache;

    public DistributedCredentialsPersistenceService(IDistributedCache cache)
    {
        this.cache = cache;
    }

    public async Task StoreAsync(string sessionId, string credentialKey, StoredCredentials credentials, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(credentials);
        var ttl = credentials.ExpiresAt - DateTime.UtcNow;
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = ttl };
        await cache.SetStringAsync(CacheKey(sessionId, credentialKey), json, options, ct);
    }

    public async Task<StoredCredentials?> GetAsync(string sessionId, string credentialKey, CancellationToken ct = default)
    {
        var json = await cache.GetStringAsync(CacheKey(sessionId, credentialKey), ct);
        if (json is null) return null;
        return JsonSerializer.Deserialize<StoredCredentials>(json);
    }

    public async Task RemoveAsync(string sessionId, string credentialKey, CancellationToken ct = default)
    {
        await cache.RemoveAsync(CacheKey(sessionId, credentialKey), ct);
    }

    private static string CacheKey(string sessionId, string credentialKey) => $"{sessionId}:{credentialKey}";
}
