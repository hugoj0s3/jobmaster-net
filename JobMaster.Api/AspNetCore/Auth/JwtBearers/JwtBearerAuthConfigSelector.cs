using Microsoft.IdentityModel.Tokens;

namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

internal sealed class JwtBearerAuthConfigSelector : IJwtBearerAuthConfigSelector
{
    private readonly JobMasterApiOptions jobMasterOptions;

    public JwtBearerAuthConfigSelector(JobMasterApiOptions jobMasterOptions)
    {
        jobMasterOptions.RequireAuthentication = true;
        this.jobMasterOptions = jobMasterOptions;
    }

    /// <summary>
    /// Register a custom JWT Bearer authentication provider. It excludes TokenValidationParameters configuration.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public void RegisterJwtBearerAuthProvider<T>() where T : class, IJobMasterJwtBearerAuthProvider
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.JwtBearerAuthProviderType = typeof(T);
    }

    /// <summary>
    /// Register the default JWT Bearer authentication provider. It includes TokenValidationParameters configuration.
    /// </summary>
    /// <param name="parameters"></param>
    public void RegisterDefaultJwtBearerAuthProvider(TokenValidationParameters parameters)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.TokenValidationParameters = parameters;
        this.jobMasterOptions.JwtBearerOptions!.JwtBearerAuthProviderType = typeof(JobMasterDefaultJwtAuthProvider);
    }

    /// <summary>
    /// Configure the Authorization header name.
    /// </summary>
    /// <param name="headerName">The header name to use for Authorization authentication. Default is "Authorization".</param>
    public void AuthorizationHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.AuthorizationHeaderName = headerName;
    }

    /// <summary>
    /// Configure the JWT Bearer scheme.
    /// </summary>
    /// <param name="scheme">The scheme to use for JWT Bearer authentication. Default is "Bearer".</param>
    public void Scheme(string scheme)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.Scheme = scheme;
    }
}