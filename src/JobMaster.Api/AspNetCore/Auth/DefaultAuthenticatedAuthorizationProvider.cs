using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace JobMaster.Api.AspNetCore.Auth;

/// Default implementation: 
/// - If authentication is NOT required, access is always granted.
/// - If authentication IS required, identity must be authenticated.
/// </summary>
internal sealed class DefaultAuthenticatedAuthorizationProvider : IJobMasterAuthorizationProvider
{
    private readonly IOptions<JobMasterApiOptions> options;

    public DefaultAuthenticatedAuthorizationProvider(IOptions<JobMasterApiOptions> options)
    {
        this.options = options;
    }
    
    public Task<bool> IsAuthorizedAsync(JobMasterApiIdentity identity, HttpContext context) 
    {
        // Safety net: if the global toggle is off, we don't block anything
        if (!options.Value.RequireAuthentication) 
        {
            return Task.FromResult(true);
        }
        
        // If required, we strictly check the authentication status
        return Task.FromResult(identity.IsAuthenticated);
    }
}