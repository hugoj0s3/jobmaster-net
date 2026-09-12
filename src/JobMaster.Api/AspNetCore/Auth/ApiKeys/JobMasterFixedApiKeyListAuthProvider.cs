using JobMaster.Api.AspNetCore.Auth;
using Microsoft.Extensions.Options;
using System;
using System.Linq;

namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

internal sealed class JobMasterFixedApiKeyListAuthProvider : IJobMasterApiKeyAuthProvider
{
    private readonly IOptions<JobMasterApiOptions> apiOptions;

    public JobMasterFixedApiKeyListAuthProvider(IOptions<JobMasterApiOptions> apiOptions)
    {
        this.apiOptions = apiOptions ?? throw new ArgumentNullException(nameof(apiOptions));
    }
    
    public Task<JobMasterApiKeyIdentity?> GetApiKeyIdentityAsync(string key)
    {
        var options = apiOptions.Value;
        var apiKeyOptions = options.ApiKeyOptions;

        if (apiKeyOptions == null)
        {
            return Task.FromResult<JobMasterApiKeyIdentity?>(null);
        }

        var result = apiKeyOptions.FixedIdentityList.FirstOrDefault(k => string.Equals(k.Key, key, StringComparison.Ordinal));
        return Task.FromResult(result);
    }
}