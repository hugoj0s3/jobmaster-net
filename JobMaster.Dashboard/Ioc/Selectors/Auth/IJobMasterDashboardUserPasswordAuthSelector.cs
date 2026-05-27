namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

public interface IJobMasterDashboardUserPasswordAuthSelector : IJobMasterDashboardAuthProviderSelector<IJobMasterDashboardUserPasswordAuthSelector>
{

    /// <summary>
    /// Sets the HTTP header name used to pass the username.
    /// </summary>
    IJobMasterDashboardUserPasswordAuthSelector WithUserHeaderName(string headerName);

    /// <summary>
    /// Sets the HTTP header name used to pass the password.
    /// </summary>
    IJobMasterDashboardUserPasswordAuthSelector WithPasswordHeaderName(string headerName);
}
