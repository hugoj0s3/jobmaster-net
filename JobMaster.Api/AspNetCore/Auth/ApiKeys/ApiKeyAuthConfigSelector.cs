namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

internal class ApiKeyAuthConfigSelector : IApiKeyAuthConfigSelector
{
    private readonly JobMasterApiOptions jobMasterOptions;

    public ApiKeyAuthConfigSelector(JobMasterApiOptions jobMasterOptions)
    {
        jobMasterOptions.RequireAuthentication = true;
        this.jobMasterOptions = jobMasterOptions;
    }
    
    /// <summary>
    /// Add a fixed API key identity.
    /// </summary>
    /// <param name="ownerName"></param>
    /// <param name="key"></param>
    /// <param name="claims"></param>
    public void AddApiKey(string ownerName, string key, IDictionary<string, string>? claims = null)
    {
        this.jobMasterOptions.EnsureApiKeyOptionsIsEnabled();
        this.jobMasterOptions.ApiKeyOptions!.FixedIdentityList.Add(
            new JobMasterApiKeyIdentity
            {
                Key = key, 
                OwnerName = ownerName,
                Claims = claims
            });
    }

    /// <summary>
    /// Register a custom API key authentication provider.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public void RegisterApiKeyAuthProvider<T>() where T : class, IJobMasterApiKeyAuthProvider
    {
        this.jobMasterOptions.EnsureApiKeyOptionsIsEnabled();
        this.jobMasterOptions.ApiKeyOptions!.ApiKeyAuthProviderType = typeof(T);
    }

    /// <summary>
    /// Configure the API key header name.
    /// </summary>
    /// <param name="headerName">The header name to use for API key authentication. Default is "X-Api-Key".</param>
    public void ApiKeyHeader(string headerName)
    {
        this.jobMasterOptions.EnsureApiKeyOptionsIsEnabled();
        this.jobMasterOptions.ApiKeyOptions!.ApiKeyHeader = headerName;
    }
}