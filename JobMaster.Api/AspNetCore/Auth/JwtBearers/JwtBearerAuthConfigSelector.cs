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

    public void RegisterJwtBearerAuthProvider<T>() where T : class, IJobMasterJwtBearerAuthProvider
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.JwtBearerAuthProviderType = typeof(T);
    }

    public void RegisterDefaultJwtBearerAuthProvider(TokenValidationParameters parameters)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.TokenValidationParameters = parameters;
        this.jobMasterOptions.JwtBearerOptions!.JwtBearerAuthProviderType = typeof(JobMasterDefaultJwtAuthProvider);
    }

    public void AuthorizationHeaderName(string headerName)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.AuthorizationHeaderName = headerName;
    }

    public void Scheme(string scheme)
    {
        this.jobMasterOptions.EnsureJwtBearerOptionsIsEnabled();
        this.jobMasterOptions.JwtBearerOptions!.Scheme = scheme;
    }
}