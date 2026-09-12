using JobMaster.Api.AspNetCore.Auth;

namespace JobMaster.Api.ApiModels;

/// <summary>Identity of the authenticated caller, as resolved by the configured authentication provider.</summary>
public class ApiWhoAmIModel
{
    /// <summary>The caller's subject — an API key's owner name, a username, or a JWT's subject claim.</summary>
    public string? Subject { get; set; }
    /// <summary>The authentication mechanism that resolved this identity.</summary>
    public JobMasterApiAuthenticationType? AuthenticationType { get; set; }
}
