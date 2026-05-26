namespace JobMaster.Api.AspNetCore.Auth.ApiKeys;

/// <summary>
/// Resolves an API key to a <see cref="JobMasterApiKeyIdentity"/>.
/// Implement this interface to provide custom key storage, rotation, or claims mapping.
/// Register via <see cref="IApiKeyAuthConfigSelector.RegisterApiKeyAuthProvider{T}"/>.
/// </summary>
public interface IJobMasterApiKeyAuthProvider
{
    /// <summary>
    /// Looks up the identity associated with <paramref name="key"/>.
    /// Returns <c>null</c> if the key is not recognized.
    /// </summary>
    Task<JobMasterApiKeyIdentity?> GetApiKeyIdentityAsync(string key);
}