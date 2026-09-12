using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.AspNetCore.Auth;

/// <summary>
/// Resolves the caller identity from an incoming HTTP request.
/// Implement this interface and register it via <see cref="JobMasterApiOptions.UseCustomizeJobMasterIdentityProvider{T}"/>
/// to replace the built-in API key / user-password / JWT bearer identity resolution.
/// </summary>
public interface IJobMasterIdentityProvider
{
    /// <summary>Resolves the <see cref="JobMasterApiIdentity"/> for the current HTTP request.</summary>
    ValueTask<JobMasterApiIdentity> GetIdentityAsync(HttpContext httpContext, CancellationToken cancellationToken = default);
}
