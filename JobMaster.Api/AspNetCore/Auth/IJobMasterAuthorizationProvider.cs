using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.AspNetCore.Auth;

public interface IJobMasterAuthorizationProvider
{
    /// <summary>
    /// Validates if the authenticated identity has permission to access JobMaster resources.
    /// </summary>
    Task<bool> IsAuthorizedAsync(JobMasterApiIdentity identity, HttpContext context);
}