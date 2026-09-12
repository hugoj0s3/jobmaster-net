using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// Server-side, distributed implementation: the cookie carries only an opaque flow id, and the
/// actual state is stored in the distributed cache, deleted on first consume — true single-use,
/// correct even behind a load balancer with multiple Dashboard replicas.
/// </summary>
internal sealed class DistributedOAuthFlowStateStorage : IJobMasterOAuthFlowStateStorage
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);
    private readonly IDistributedCache cache;

    public DistributedOAuthFlowStateStorage(IDistributedCache cache)
    {
        this.cache = cache;
    }

    public async Task<string> BeginAsync(OAuthFlowState state)
    {
        var flowId = Guid.NewGuid().ToString("N");
        var json = JsonSerializer.Serialize(state);
        var options = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = Ttl };
        await cache.SetStringAsync(CacheKey(flowId), json, options);
        return flowId;
    }

    public async Task<OAuthFlowState?> ConsumeAsync(string cookieValue)
    {
        var key = CacheKey(cookieValue);
        var json = await cache.GetStringAsync(key);
        await cache.RemoveAsync(key);
        return json is null ? null : JsonSerializer.Deserialize<OAuthFlowState>(json);
    }

    private static string CacheKey(string flowId) => $"jm-oauth-flow:{flowId}";
}
