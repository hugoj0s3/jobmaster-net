namespace JobMaster.Dashboard.AuthRetention;

internal interface IAuthRetentionService
{
    Task StoreAsync(string sessionId, string authKey, StoredAuth credentials, CancellationToken ct = default);
    Task<StoredAuth?> GetAsync(string sessionId, string authKey, CancellationToken ct = default);
    Task RemoveAsync(string sessionId, string authKey, CancellationToken ct = default);
}
