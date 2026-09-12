using Microsoft.Extensions.Caching.Memory;

namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// Server-side, single-instance implementation: the cookie carries only an opaque flow id,
/// and the actual state is stored in-memory, deleted on first consume — true single-use.
/// </summary>
internal sealed class InMemoryOAuthFlowStateStorage : IJobMasterOAuthFlowStateStorage
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);
    private readonly IMemoryCache cache;

    public InMemoryOAuthFlowStateStorage(IMemoryCache cache)
    {
        this.cache = cache;
    }

    public Task<string> BeginAsync(OAuthFlowState state)
    {
        var flowId = Guid.NewGuid().ToString("N");
        cache.Set(CacheKey(flowId), state, Ttl);
        return Task.FromResult(flowId);
    }

    public Task<OAuthFlowState?> ConsumeAsync(string cookieValue)
    {
        var key = CacheKey(cookieValue);
        cache.TryGetValue(key, out OAuthFlowState? state);
        cache.Remove(key);
        return Task.FromResult(state);
    }

    private static string CacheKey(string flowId) => $"jm-oauth-flow:{flowId}";
}
