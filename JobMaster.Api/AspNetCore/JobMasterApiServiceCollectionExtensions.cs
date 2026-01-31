using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Auth.ApiKeys;
using JobMaster.Api.AspNetCore.Auth.UsersAndPasswords;
using JobMaster.Api.AspNetCore.Swagger;
using JobMaster.Api.AspNetCore.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using JobMaster.Api.AspNetCore.Auth.JwtBearers;
using JobMaster.Sdk.Utils.Extensions;
using Microsoft.AspNetCore.Builder;

namespace JobMaster.Api.AspNetCore;

public static class JobMasterApiServiceCollectionExtensions
{
    public static IServiceCollection UseJobMasterApi(this IServiceCollection services, Action<JobMasterApiOptions>? configureAuth = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        var options = new JobMasterApiOptions();
        configureAuth?.Invoke(options);
        options.BasePath = JobMasterApiPath.NormalizeBasePath(options.BasePath);

        ValidateAuthOptions(options);

        if (configureAuth != null)
        {
            services.Configure(configureAuth);
        }

        services.PostConfigure<JobMasterApiOptions>(o =>
        {
            o.BasePath = JobMasterApiPath.NormalizeBasePath(o.BasePath);
        });

        if (options.EnableSwagger)
        {
            JobMasterApiSwaggerSupport.ConfigureServices(services, options);
        }

        if (options.UserPwdOptions?.UserPwdAuthProviderType is not null)
        {
            services.AddScoped(typeof(IJobMasterUserPwdAuthProvider), options.UserPwdOptions.UserPwdAuthProviderType);
        }
        services.AddScoped(typeof(JobMasterFixedUserPwdListAuthProvider), typeof(JobMasterFixedUserPwdListAuthProvider));
        
        if (options.ApiKeyOptions?.ApiKeyAuthProviderType is not null)
        {
            services.AddScoped(typeof(IJobMasterApiKeyAuthProvider), options.ApiKeyOptions.ApiKeyAuthProviderType);
        }
        services.AddScoped(typeof(JobMasterFixedApiKeyListAuthProvider), typeof(JobMasterFixedApiKeyListAuthProvider));

        if (options.JwtBearerOptions?.JwtBearerAuthProviderType is not null)
        {
            services.AddScoped(typeof(IJobMasterJwtBearerAuthProvider), options.JwtBearerOptions.JwtBearerAuthProviderType);
        }

        if (options.JobMasterIdentityProviderType == null)
        {
            services.AddScoped<IJobMasterIdentityProvider, JobMasterApiIdentityProvider>();
        } 
        else
        {
            services.AddScoped(typeof(IJobMasterIdentityProvider), options.JobMasterIdentityProviderType);
        }

        if (options.JobMasterAuthorizationProviderType == null)
        {
            services.AddScoped<IJobMasterAuthorizationProvider, DefaultAuthenticatedAuthorizationProvider>();
        }
        else
        {
            services.AddScoped(typeof(IJobMasterAuthorizationProvider), options.JobMasterAuthorizationProviderType);
        }
        
        return services;
    }

    private static void ValidateAuthOptions(JobMasterApiOptions options)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));

        var supported = options.GetAuthenticationTypesSupported();
        if (supported.IsNullOrEmpty())
        {
            return;
        }

        var usedHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (supported.Contains(JobMasterApiAuthenticationType.ApiKey))
        {
            var apiKeyOptions = options.ApiKeyOptions ?? throw new InvalidOperationException("ApiKey auth is enabled but ApiKeyOptions is null.");

            if (string.IsNullOrWhiteSpace(apiKeyOptions.ApiKeyHeader))
            {
                throw new InvalidOperationException("ApiKey auth is enabled but ApiKeyHeader is empty.");
            }

            EnsureUniqueHeader(usedHeaders, apiKeyOptions.ApiKeyHeader, "ApiKeyHeader");
        }

        if (supported.Contains(JobMasterApiAuthenticationType.UserPwd))
        {
            var userPwdOptions = options.UserPwdOptions ?? throw new InvalidOperationException("UserPwd auth is enabled but UserPwdOptions is null.");

            if (string.IsNullOrWhiteSpace(userPwdOptions.UserHeaderName))
            {
                throw new InvalidOperationException("UserPwd auth is enabled but UserHeaderName is empty.");
            }

            if (string.IsNullOrWhiteSpace(userPwdOptions.PwdHeaderName))
            {
                throw new InvalidOperationException("UserPwd auth is enabled but PwdHeaderName is empty.");
            }

            EnsureUniqueHeader(usedHeaders, userPwdOptions.UserHeaderName, "UserHeaderName");
            EnsureUniqueHeader(usedHeaders, userPwdOptions.PwdHeaderName, "PwdHeaderName");
        }

        if (supported.Contains(JobMasterApiAuthenticationType.JwtBearer))
        {
            var jwtOptions = options.JwtBearerOptions ?? throw new InvalidOperationException("JwtBearer auth is enabled but JwtBearerOptions is null.");

            var headerName = jwtOptions.AuthorizationHeaderName;
            if (string.IsNullOrWhiteSpace(headerName))
            {
                headerName = "Authorization";
            }

            var scheme = string.IsNullOrWhiteSpace(jwtOptions.Scheme) ? "Bearer" : jwtOptions.Scheme;

            if (string.Equals(scheme, "Bearer", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(headerName, "Authorization", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("JwtBearer auth uses scheme 'Bearer' and must use header 'Authorization'.");
            }

            EnsureUniqueHeader(usedHeaders, headerName, "AuthorizationHeaderName");
        }
    }

    private static void EnsureUniqueHeader(HashSet<string> usedHeaders, string headerName, string optionName)
    {
        if (usedHeaders == null) throw new ArgumentNullException(nameof(usedHeaders));
        if (optionName == null) throw new ArgumentNullException(nameof(optionName));

        if (!usedHeaders.Add(headerName))
        {
            throw new InvalidOperationException($"Header name conflict: '{headerName}' is used by multiple auth mechanisms (conflict detected while processing {optionName}).");
        }
    }
}
