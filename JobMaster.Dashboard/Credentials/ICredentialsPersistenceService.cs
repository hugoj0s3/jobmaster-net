namespace JobMaster.Dashboard.Credentials;

internal interface ICredentialsPersistenceService
{
    Task StoreAsync(string sessionId, string credentialKey, StoredCredentials credentials, CancellationToken ct = default);
    Task<StoredCredentials?> GetAsync(string sessionId, string credentialKey, CancellationToken ct = default);
    Task RemoveAsync(string sessionId, string credentialKey, CancellationToken ct = default);
}
