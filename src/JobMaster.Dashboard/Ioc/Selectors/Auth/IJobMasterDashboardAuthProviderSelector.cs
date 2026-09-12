namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

public interface IJobMasterDashboardAuthProviderSelector<out TReturn>
{
    /// <summary>
    /// Sets a display name shown in the dashboard UI for this auth provider.
    /// </summary>
    TReturn WithDisplayName(string displayName);
}
