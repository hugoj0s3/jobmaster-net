using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;

namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// Self-contained implementation: seals the flow state directly into the value handed back as
/// the cookie via ASP.NET Core Data Protection. No server-side storage needed, which also means
/// no genuine single-use guarantee (a captured raw cookie value could in principle be replayed
/// within its short window) — an accepted trade-off for needing no infrastructure at all.
/// </summary>
internal sealed class CookieOAuthFlowStateStorage : IJobMasterOAuthFlowStateStorage
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);
    private readonly ITimeLimitedDataProtector protector;

    public CookieOAuthFlowStateStorage(IDataProtectionProvider dataProtectionProvider)
    {
        protector = dataProtectionProvider
            .CreateProtector("JobMaster.Dashboard.OAuthFlow")
            .ToTimeLimitedDataProtector();
    }

    public Task<string> BeginAsync(OAuthFlowState state)
    {
        var json = JsonSerializer.Serialize(state);
        var protectedValue = protector.Protect(json, DateTimeOffset.UtcNow.Add(Ttl));
        return Task.FromResult(protectedValue);
    }

    public Task<OAuthFlowState?> ConsumeAsync(string cookieValue)
    {
        try
        {
            var json = protector.Unprotect(cookieValue);
            return Task.FromResult(JsonSerializer.Deserialize<OAuthFlowState>(json));
        }
        catch
        {
            return Task.FromResult<OAuthFlowState?>(null);
        }
    }
}
