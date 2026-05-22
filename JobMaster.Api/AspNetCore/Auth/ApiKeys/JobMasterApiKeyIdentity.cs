namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

/// <summary>Represents the identity associated with a registered API key.</summary>
public sealed class JobMasterApiKeyIdentity
{
    /// <summary>The secret API key value.</summary>
    public string Key { get; set; } = string.Empty;
    /// <summary>A human-readable label for the key owner.</summary>
    public string OwnerName { get; set; } = string.Empty;
    /// <summary>Optional claims attached to this identity.</summary>
    public IDictionary<string, string>? Claims = null;
}