namespace JobMaster.Api.AspNetCore.Auth;

/// <summary>Identifies the authentication mechanism used to verify a caller's identity.</summary>
public enum JobMasterApiAuthenticationType
{
    /// <summary>Authentication via username and password headers.</summary>
    UserPwd,
    /// <summary>Authentication via a static or dynamic API key header.</summary>
    ApiKey,
    /// <summary>Authentication via a JWT Bearer token in the Authorization header.</summary>
    JwtBearer,
    /// <summary>Authentication via a custom <see cref="IJobMasterIdentityProvider"/> implementation.</summary>
    Customized,
}