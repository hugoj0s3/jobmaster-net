using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.AspNetCore.Auth;

/// <summary>
/// Determines whether an authenticated identity is authorized to access JobMaster API resources.
/// Implement this interface and register it via <see cref="JobMasterApiOptions.UseCustomizeJobMasterAuthorizationProvider{T}"/>
/// to replace the default all-or-nothing authorization behavior.
/// </summary>
public interface IJobMasterAuthorizationProvider
{
    /// <summary>
    /// Validates if the authenticated identity has permission to access JobMaster resources.
    /// </summary>
    Task<bool> IsAuthorizedAsync(JobMasterApiIdentity identity, HttpContext context);
}