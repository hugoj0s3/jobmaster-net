using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.AspNetCore.Auth;

public interface IJobMasterIdentityProvider
{
    ValueTask<JobMasterApiIdentity> GetIdentityAsync(HttpContext httpContext, CancellationToken cancellationToken = default);
}
