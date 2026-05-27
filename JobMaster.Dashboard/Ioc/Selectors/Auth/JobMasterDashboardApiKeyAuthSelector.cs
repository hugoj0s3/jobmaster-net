using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal class JobMasterDashboardApiKeyAuthSelector : IJobMasterDashboardApiKeyAuthSelector
{
    private readonly ApiKeyAuthProviderConfig config;

    public JobMasterDashboardApiKeyAuthSelector(ApiKeyAuthProviderConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardApiKeyAuthSelector WithDisplayName(string displayName)
    {
        this.config.DisplayName = displayName;
        return this;
    }

    public IJobMasterDashboardApiKeyAuthSelector WithHeaderName(string headerName)
    {
        this.config.HeaderName = headerName;
        return this;
    }
}
