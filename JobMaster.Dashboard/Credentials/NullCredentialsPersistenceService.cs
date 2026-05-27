namespace JobMaster.Dashboard.Credentials;

internal sealed class NullCredentialsPersistenceService : ICredentialsPersistenceService
{
    private const string Message =
        "Server-side credential persistence is not configured. " +
        "Set PersistenceType to ServerSideInMemory or ServerSideDistributed in AddJobMasterDashboard.";

    public Task StoreAsync(string sessionId, string credentialKey, StoredCredentials credentials, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);

    public Task<StoredCredentials?> GetAsync(string sessionId, string credentialKey, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);

    public Task RemoveAsync(string sessionId, string credentialKey, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);
}
