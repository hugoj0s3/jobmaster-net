namespace JobMaster.Dashboard.AuthRetention;

internal sealed class NullAuthRetentionStorage : IJobMasterAuthRetentionStorage
{
    private const string Message =
        "Server-side auth retention is not configured. " +
        "Set AuthRetentionType to ServerSideInMemory or ServerSideDistributed in AddJobMasterDashboard.";

    public Task StoreAsync(string sessionId, string authKey, RetainedCredential credentials)
        => throw new InvalidOperationException(Message);

    public Task<RetainedCredential?> GetAsync(string sessionId, string authKey)
        => throw new InvalidOperationException(Message);

    public Task RemoveAsync(string sessionId, string authKey)
        => throw new InvalidOperationException(Message);
}
