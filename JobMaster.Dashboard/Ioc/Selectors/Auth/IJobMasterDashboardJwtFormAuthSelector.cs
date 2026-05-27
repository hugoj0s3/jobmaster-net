using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

public interface IJobMasterDashboardJwtFormAuthSelector : IJobMasterDashboardAuthProviderSelector<IJobMasterDashboardJwtFormAuthSelector>
{
    /// <summary>
    /// Sets the token endpoint URL that the dashboard will POST to.
    /// </summary>
    IJobMasterDashboardJwtFormAuthSelector WithTokenUrl(string tokenUrl);

    /// <summary>
    /// Sets the HTTP header name used to pass the obtained JWT token. Defaults to "Authorization".
    /// </summary>
    IJobMasterDashboardJwtFormAuthSelector WithHeaderName(string headerName);

    /// <summary>
    /// Sets the authentication scheme prefixed before the token. Defaults to "Bearer".
    /// </summary>
    IJobMasterDashboardJwtFormAuthSelector WithScheme(string scheme);

    IJobMasterDashboardJwtFormAuthSelector AddField(
        string id,
        string label,
        DashboardJwtFormFieldType type = DashboardJwtFormFieldType.Text,
        bool isRequired = true,
        string? defaultValue = null,
        bool isDisabled = false
    );
}
