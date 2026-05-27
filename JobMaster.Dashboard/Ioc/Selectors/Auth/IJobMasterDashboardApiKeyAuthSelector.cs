namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

public interface IJobMasterDashboardApiKeyAuthSelector : IJobMasterDashboardAuthProviderSelector<IJobMasterDashboardApiKeyAuthSelector>
{

    /// <summary>
    /// Sets the HTTP header name used to pass the API key.
    /// </summary>
    IJobMasterDashboardApiKeyAuthSelector WithHeaderName(string headerName);
}