namespace JobMaster.Api.AspNetCore.Auth;

public sealed record JobMasterApiIdentity(
    bool IsAuthenticated,
    string? Subject,
    JobMasterApiAuthenticationType? AuthenticationType,
    IDictionary<string, string>? Claims = null
);
