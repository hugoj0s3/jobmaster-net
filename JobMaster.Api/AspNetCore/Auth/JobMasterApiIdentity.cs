namespace JobMaster.Api.AspNetCore.Auth;

/// <summary>Immutable snapshot of the authenticated caller's identity resolved from an HTTP request.</summary>
public sealed record JobMasterApiIdentity(
    bool IsAuthenticated,
    string? Subject,
    JobMasterApiAuthenticationType? AuthenticationType,
    IDictionary<string, string>? Claims = null
);
