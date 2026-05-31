namespace JobMaster.Dashboard.AuthRetention;

internal sealed class NullAuthRetentionService : IAuthRetentionService
{
    private const string Message =
        "Server-side auth retention is not configured. " +
        "Set AuthRetentionType to ServerSideInMemory or ServerSideDistributed in AddJobMasterDashboard.";

    public Task StoreAsync(string sessionId, string authKey, StoredAuth credentials, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);

    public Task<StoredAuth?> GetAsync(string sessionId, string authKey, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);

    public Task RemoveAsync(string sessionId, string authKey, CancellationToken ct = default)
        => throw new InvalidOperationException(Message);
}
