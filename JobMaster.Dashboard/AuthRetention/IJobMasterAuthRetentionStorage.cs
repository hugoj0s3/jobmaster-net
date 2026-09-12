namespace JobMaster.Dashboard.AuthRetention;

/// <summary>
/// Stores, retrieves, and removes dashboard credentials retained across page refreshes.
/// Implement this to plug in a custom storage backend via
/// <see cref="Ioc.Selectors.AuthRetention.IJobMasterDashboardAuthRetentionSelector.UseCustom{T}"/>.
/// </summary>
public interface IJobMasterAuthRetentionStorage
{
    Task StoreAsync(string sessionId, string authKey, RetainedCredential credentials);
    Task<RetainedCredential?> GetAsync(string sessionId, string authKey);
    Task RemoveAsync(string sessionId, string authKey);
}
