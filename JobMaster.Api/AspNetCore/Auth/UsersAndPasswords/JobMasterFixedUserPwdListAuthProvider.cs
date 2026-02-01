using JobMaster.Api.AspNetCore.Auth;
using Microsoft.Extensions.Options;
using System;
using System.Linq;

namespace JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;

internal sealed class JobMasterFixedUserPwdListAuthProvider : IJobMasterUserPwdAuthProvider
{
    private readonly IOptions<JobMasterApiOptions> apiOptions;

    public JobMasterFixedUserPwdListAuthProvider(IOptions<JobMasterApiOptions> apiOptions)
    {
        this.apiOptions = apiOptions ?? throw new ArgumentNullException(nameof(apiOptions));
    }
    
    public Task<JobMasterUserPwdIdentity?> GetUserPwdIdentityAsync(string userName)
    {
        var options = apiOptions.Value;
        var userPwdOptions = options.UserPwdOptions;
        if (userPwdOptions == null)
        {
            return Task.FromResult<JobMasterUserPwdIdentity?>(null);
        }

        var result = userPwdOptions.FixedIdentityPlanPwdList.FirstOrDefault(k => string.Equals(k.UserName, userName, StringComparison.Ordinal));
        return Task.FromResult(result);
    }

    public bool Verify(string planPassword, string hashedPassword)
    {
        return JobMasterPasswordHasher.Verify(planPassword, hashedPassword);
    }
}