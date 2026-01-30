namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

internal class ApiKeyAuthConfigSelector : IApiKeyAuthConfigSelector
{
    private readonly JobMasterApiOptions jobMasterOptions;

    public ApiKeyAuthConfigSelector(JobMasterApiOptions jobMasterOptions)
    {
        jobMasterOptions.RequireAuthentication = true;
        this.jobMasterOptions = jobMasterOptions;
    }
    
    public void AddApiKey(string key, string ownerName, IDictionary<string, string>? claims = null)
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

    public void RegisterApiKeyAuthProvider<T>() where T : class, IJobMasterApiKeyAuthProvider
    {
        this.jobMasterOptions.EnsureApiKeyOptionsIsEnabled();
        this.jobMasterOptions.ApiKeyOptions!.ApiKeyAuthProviderType = typeof(T);
    }

    public void ApiKeyHeader(string headerName)
    {
        this.jobMasterOptions.EnsureApiKeyOptionsIsEnabled();
        this.jobMasterOptions.ApiKeyOptions!.ApiKeyHeader = headerName;
    }
}