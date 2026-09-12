namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

public interface IJobMasterDashboardSimpleJwtAuthSelector : IJobMasterDashboardAuthProviderSelector<IJobMasterDashboardSimpleJwtAuthSelector>
{

    /// <summary>
    /// Sets the HTTP header name used to pass the JWT token. Defaults to "Authorization".
    /// </summary>
    IJobMasterDashboardSimpleJwtAuthSelector WithHeaderName(string headerName);

    /// <summary>
    /// Sets the authentication scheme prefixed before the token. Defaults to "Bearer".
    /// </summary>
    IJobMasterDashboardSimpleJwtAuthSelector WithScheme(string scheme);
}