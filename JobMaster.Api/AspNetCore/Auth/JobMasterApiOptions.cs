using JobMaster.Api.AspNetCore.Auth.ApiKeys;
using JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;
using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Api.AspNetCore.Auth.JwtBearers;

namespace JobMaster.Api.AspNetCore.Auth;

public sealed class JobMasterApiOptions
{
    /// <summary>
    ///  Require authentication for all API endpoints. Default is false.
    /// </summary>
    public bool RequireAuthentication { get; set; } = false;
    
    /// <summary>
    ///  Base path for the JobMaster API. Default is "/jm-api".
    /// </summary>
    public string BasePath { get; set; } = "/jm-api";
    
    /// <summary>
    /// Enable Swagger UI for API documentation.
    /// </summary>
    public bool EnableSwagger { get; set; } = false;
    
    public IList<JobMasterApiAuthenticationType> GetAuthenticationTypesSupported()
    {
        return AuthenticationTypesSupported.ToList();
    }
    
    /// <summary>
    /// Configures API Key authentication.
    /// </summary>
    /// <returns></returns>
    public IApiKeyAuthConfigSelector UseApiKeyAuth() => new ApiKeyAuthConfigSelector(this);
    
    /// <summary>
    /// Configure User/Password authentication.
    /// </summary>
    /// <returns></returns>
    
    public IApiUserPwdAuthConfigSelector UseUserPwdAuth() => new ApiUserPwdAuthConfigSelector(this);

    /// <summary>
    /// Configure JWT Bearer authentication.
    /// </summary>
    /// <returns></returns>
    public IJwtBearerAuthConfigSelector UseJwtBearerAuth() => new JwtBearerAuthConfigSelector(this);
    
    /// <summary>
    /// Use custom JobMaster identity provider. it excludes all ApiKey, UserPwd and JwtBearer authentication configuration.
    /// </summary>
    /// <returns></returns>
    public void UseCustomizeJobMasterIdentityProvider<T>() where T : class, IJobMasterIdentityProvider
    {
        JobMasterIdentityProviderType = typeof(T);
    }
    
    /// <summary>
    /// Use custom JobMaster authorization provider. By default, is all or nothing. If the user is authenticated, it is authorized.
    /// </summary>
    /// <returns></returns>
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