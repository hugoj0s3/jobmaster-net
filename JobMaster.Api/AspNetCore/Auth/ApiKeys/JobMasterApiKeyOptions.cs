namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

internal sealed class JobMasterApiKeyOptions
{
    public string ApiKeyHeader { get; set; } = "x-api-key";
    
    public IList<JobMasterApiKeyIdentity> FixedIdentityList { get; set; } = new List<JobMasterApiKeyIdentity>();
    
    internal Type? ApiKeyAuthProviderType { get; set; }
}