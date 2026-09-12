namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

/// <summary>Represents the identity carried by a validated JWT Bearer token.</summary>
public sealed class JobMasterJwtBearerIdentity
{
    /// <summary>Subject claim identifying the token holder.</summary>
    public string Subject { get; set; } = string.Empty;
    /// <summary>Additional claims extracted from the token.</summary>
    public IDictionary<string, string> Claims { get; set; } = new Dictionary<string, string>();
}
