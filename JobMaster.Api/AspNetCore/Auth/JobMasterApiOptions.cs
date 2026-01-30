using JobMaster.Api.AspNetCore.Auth.ApiKeys;
using JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;
using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Api.AspNetCore.Auth.JwtBearers;

namespace JobMaster.Api.AspNetCore.Auth;

public sealed class JobMasterApiOptions
{
    public bool RequireAuthentication { get; set; } = false;
    public string BasePath { get; set; } = "/jm-api";
    
    public bool EnableSwagger { get; set; } = false;
    
    public IList<JobMasterApiAuthenticationType> GetAuthenticationTypesSupported()
    {
        return AuthenticationTypesSupported.ToList();
    }
    
    public IApiKeyAuthConfigSelector UseApiKeyAuth() => new ApiKeyAuthConfigSelector(this);
    
    public IApiUserPwdAuthConfigSelector UseUserPwdAuth() => new ApiUserPwdAuthConfigSelector(this);

    public IJwtBearerAuthConfigSelector UseJwtBearerAuth() => new JwtBearerAuthConfigSelector(this);
    
    public void UseCustomizeJobMasterIdentityProvider<T>() where T : class, IJobMasterIdentityProvider
    {
        JobMasterIdentityProviderType = typeof(T);
    }
    
    public void UseCustomizeJobMasterAuthorizationProvider<T>() where T : class, IJobMasterAuthorizationProvider
    {
        JobMasterAuthorizationProviderType = typeof(T);
    }

    internal void EnsureUserPwdOptionsIsEnabled()
    {
        if (UserPwdOptions == null)
        {
            UserPwdOptions = new JobMasterUserPwdOptions();
        }
        
        AuthenticationTypesSupported.Add(JobMasterApiAuthenticationType.UserPwd);
    }

    internal ISet<JobMasterApiAuthenticationType> AuthenticationTypesSupported { get; set; } 
        = new HashSet<JobMasterApiAuthenticationType>();
    
    internal JobMasterApiKeyOptions? ApiKeyOptions { get; set; } = new JobMasterApiKeyOptions();
    
    internal JobMasterUserPwdOptions? UserPwdOptions { get; set; } = new JobMasterUserPwdOptions();

    internal JobMasterJwtBearerOptions? JwtBearerOptions { get; set; } = new JobMasterJwtBearerOptions();
    
    internal void EnsureApiKeyOptionsIsEnabled()
    {
        if (ApiKeyOptions == null)
        {
            ApiKeyOptions = new JobMasterApiKeyOptions();
        }

        AuthenticationTypesSupported.Add(JobMasterApiAuthenticationType.ApiKey);
    }

    internal void EnsureJwtBearerOptionsIsEnabled()
    {
        if (JwtBearerOptions == null)
        {
            JwtBearerOptions = new JobMasterJwtBearerOptions();
        }

        AuthenticationTypesSupported.Add(JobMasterApiAuthenticationType.JwtBearer);
    }
    
    internal Type? JobMasterIdentityProviderType { get; set; }
    internal Type? JobMasterAuthorizationProviderType { get; set; }
    public bool EnableLogging { get; set; }
}