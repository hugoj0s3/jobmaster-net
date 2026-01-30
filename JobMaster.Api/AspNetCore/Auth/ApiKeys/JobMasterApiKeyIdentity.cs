namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

public sealed class JobMasterApiKeyIdentity
{
    public string Key { get; set; } = string.Empty;
    public string OwnerName { get; set; } = string.Empty;
    public IDictionary<string, string>? Claims = null;
}