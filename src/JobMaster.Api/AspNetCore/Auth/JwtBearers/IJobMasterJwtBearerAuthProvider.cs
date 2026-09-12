namespace JobMaster.Api.AspNetCore.Auth.JwtBearers;

/// <summary>
/// Validates and generates JWT Bearer tokens for the JobMaster API.
/// Implement this interface to provide custom JWT validation and issuance logic.
/// Register via <see cref="IJwtBearerAuthConfigSelector.RegisterJwtBearerAuthProvider{T}"/>.
/// </summary>
public interface IJobMasterJwtBearerAuthProvider
{
    /// <summary>
    /// Validates <paramref name="token"/> and returns the associated identity.
    /// Returns <c>null</c> if the token is invalid or expired.
    /// </summary>
    Task<JobMasterJwtBearerIdentity?> ValidateTokenAsync(string token);

    /// <summary>
    /// Issues a signed JWT for <paramref name="identity"/> with an optional <paramref name="lifetime"/>.
    /// </summary>
    string GenerateToken(JobMasterJwtBearerIdentity identity, TimeSpan? lifetime = null);
}

