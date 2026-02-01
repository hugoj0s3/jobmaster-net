using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Linq;
using JobMaster.Api.AspNetCore.Auth.ApiKeys;
using JobMaster.Api.AspNetCore.Auth.JwtBearers;
using JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Api.AspNetCore.Auth;

internal sealed class JobMasterApiIdentityProvider : IJobMasterIdentityProvider
{
    private readonly IServiceProvider serviceProvider;
    private readonly IOptions<JobMasterApiOptions> options;

    public JobMasterApiIdentityProvider(IServiceProvider serviceProvider, IOptions<JobMasterApiOptions> options)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public async ValueTask<JobMasterApiIdentity> GetIdentityAsync(HttpContext httpContext, CancellationToken cancellationToken = default)
    {
        if (httpContext == null) throw new ArgumentNullException(nameof(httpContext));
        
        var authenticationType = IdentifyAuthenticationType(httpContext);
        if (authenticationType == null)
        {
            return AnonymousUserConstants.Anonymous;
        }

        if (authenticationType == JobMasterApiAuthenticationType.ApiKey)
        {
            var apiKeyIdentity = await GetApiKeyIdentityAsync(httpContext);
            if (apiKeyIdentity != null)
            {
                return new JobMasterApiIdentity(
                    true, 
                    apiKeyIdentity.OwnerName, 
                    JobMasterApiAuthenticationType.ApiKey,
                    apiKeyIdentity.Claims);
            }
        }

        if (authenticationType == JobMasterApiAuthenticationType.UserPwd)
        {
            var userPwdIdentity = await GetUserPwdIdentityAsync(httpContext);
            if (userPwdIdentity != null)
            {
                return new JobMasterApiIdentity(
                    true, 
                    userPwdIdentity.UserName, 
                    JobMasterApiAuthenticationType.UserPwd,
                    userPwdIdentity.Claims);
            }
        }

        if (authenticationType == JobMasterApiAuthenticationType.JwtBearer)
        {
            var jwtIdentity = await GetJwtBearerIdentityAsync(httpContext);
            if (jwtIdentity != null && !string.IsNullOrWhiteSpace(jwtIdentity.Subject))
            {
                return new JobMasterApiIdentity(
                    true, 
                    jwtIdentity.Subject, 
                    JobMasterApiAuthenticationType.JwtBearer,
                    jwtIdentity.Claims);
            }
        }
        
        return AnonymousUserConstants.Anonymous;
    }

    private async Task<JobMasterJwtBearerIdentity?> GetJwtBearerIdentityAsync(HttpContext httpContext)
    {
        var provider = this.serviceProvider.GetService<IJobMasterJwtBearerAuthProvider>();
        if (provider == null) return null;

        var optionsValue = this.options.Value;
        var jwtOptions = optionsValue.JwtBearerOptions;
        if (jwtOptions == null)
        {
            return null;
        }

        var headerName = jwtOptions.AuthorizationHeaderName;
        if (string.IsNullOrWhiteSpace(headerName))
        {
            headerName = "Authorization";
        }

        var rawHeader = httpContext.Request.Headers[headerName].ToString();
        if (string.IsNullOrWhiteSpace(rawHeader))
        {
            return null;
        }

        var scheme = string.IsNullOrWhiteSpace(jwtOptions.Scheme) ? "Bearer" : jwtOptions.Scheme;
        var prefix = scheme + " ";

        var token = rawHeader.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? rawHeader.Substring(prefix.Length).Trim()
            : rawHeader.Trim();

        if (string.IsNullOrWhiteSpace(token))
        {
            return null;
        }

        var identity = await provider.ValidateTokenAsync(token);
        return identity;
    }

    private async Task<JobMasterUserPwdIdentity?> GetUserPwdIdentityAsync(HttpContext httpContext)
    {
        var customProvider = this.serviceProvider.GetService<IJobMasterUserPwdAuthProvider>();
        var fixedProvider = this.serviceProvider.GetRequiredService<JobMasterFixedUserPwdListAuthProvider>();
        
        var userName = httpContext.Request.Headers[this.options.Value.UserPwdOptions!.UserHeaderName];
        var planPwd = httpContext.Request.Headers[this.options.Value.UserPwdOptions!.PwdHeaderName];

        if (string.IsNullOrEmpty(userName) || string.IsNullOrEmpty(planPwd))
        {
            return null;
        }

        JobMasterUserPwdIdentity? identity = null;

        if (customProvider is not null)
        {
            identity = await customProvider.GetUserPwdIdentityAsync(userName!);
            if (identity != null && customProvider.Verify(planPwd!, identity.HashedPassword))
            {
                return identity;
            }
        }

        identity = await fixedProvider.GetUserPwdIdentityAsync(userName!);
        if (identity != null && fixedProvider.Verify(planPwd!, identity.HashedPassword))
        {
            return identity;
        }

        return null;
    }

    private async Task<JobMasterApiKeyIdentity?> GetApiKeyIdentityAsync(HttpContext httpContext)
    {
        var customProvider = this.serviceProvider.GetService<IJobMasterApiKeyAuthProvider>();
        var fixedProvider = this.serviceProvider.GetRequiredService<JobMasterFixedApiKeyListAuthProvider>();
        
        var apiKey = httpContext.Request.Headers[this.options.Value.ApiKeyOptions!.ApiKeyHeader];
        if (string.IsNullOrEmpty(apiKey))
        {
            return null;
        }
        
        JobMasterApiKeyIdentity? result = null;
        if (customProvider is not null)
        {
            result = await customProvider.GetApiKeyIdentityAsync(apiKey!);
        }
        
        if (result is not null)
        {
            return result;
        }
        
        result = await fixedProvider.GetApiKeyIdentityAsync(apiKey!);
        
        return result;
    }
    
    private JobMasterApiAuthenticationType? IdentifyAuthenticationType(HttpContext httpContext)
    {
        if (IsApiKey(httpContext))
        {
            return JobMasterApiAuthenticationType.ApiKey;
        }
        
        if (IsUserPwd(httpContext))
        {
            return JobMasterApiAuthenticationType.UserPwd;
        }

        if (IsJwtBearer(httpContext))
        {
            return JobMasterApiAuthenticationType.JwtBearer;
        }
        
        return null;
    }
    
    private bool IsApiKey(HttpContext httpContext)
    {
        var optionsValue = this.options.Value ?? new JobMasterApiOptions();

        if (!optionsValue.GetAuthenticationTypesSupported().Contains(JobMasterApiAuthenticationType.ApiKey))
        {
            return false;
        }
        
        var headerName = optionsValue.ApiKeyOptions?.ApiKeyHeader;
        if (string.IsNullOrEmpty(headerName))
        {
            return false;
        }
        
        return httpContext.Request.Headers.TryGetValue(headerName, out var _);
    }

    private bool IsUserPwd(HttpContext httpContext)
    {
        
        var optionsValue = this.options.Value ?? new JobMasterApiOptions();

        if (!optionsValue.GetAuthenticationTypesSupported().Contains(JobMasterApiAuthenticationType.UserPwd))
        {
            return false;
        }

        var userHeaderName = optionsValue.UserPwdOptions?.UserHeaderName;
        var pwdHeaderName = optionsValue.UserPwdOptions?.PwdHeaderName;
        
        if (string.IsNullOrEmpty(userHeaderName) || string.IsNullOrEmpty(pwdHeaderName))
        {
            return false;
        }
        
        return httpContext.Request.Headers.TryGetValue(userHeaderName, out var _) && 
               httpContext.Request.Headers.TryGetValue(pwdHeaderName, out var _);
    }

    private bool IsJwtBearer(HttpContext httpContext)
    {
        var optionsValue = this.options.Value ?? new JobMasterApiOptions();

        if (!optionsValue.GetAuthenticationTypesSupported().Contains(JobMasterApiAuthenticationType.JwtBearer))
        {
            return false;
        }

        var jwtOptions = optionsValue.JwtBearerOptions;
        if (jwtOptions == null)
        {
            return false;
        }

        var headerName = jwtOptions.AuthorizationHeaderName;
        if (string.IsNullOrWhiteSpace(headerName))
        {
            headerName = "Authorization";
        }

        if (!httpContext.Request.Headers.TryGetValue(headerName, out var raw) || string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var scheme = string.IsNullOrWhiteSpace(jwtOptions.Scheme) ? "Bearer" : jwtOptions.Scheme;
        return raw.ToString().StartsWith(scheme + " ", StringComparison.OrdinalIgnoreCase);
    }
}
