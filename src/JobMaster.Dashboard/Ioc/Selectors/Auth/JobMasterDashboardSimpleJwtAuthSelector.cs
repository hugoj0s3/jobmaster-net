using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal class JobMasterDashboardSimpleJwtAuthSelector : IJobMasterDashboardSimpleJwtAuthSelector
{
    private readonly SimpleJwtAuthProviderConfig config;

    public JobMasterDashboardSimpleJwtAuthSelector(SimpleJwtAuthProviderConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardSimpleJwtAuthSelector WithDisplayName(string displayName)
    {
        this.config.DisplayName = displayName;
        return this;
    }

    public IJobMasterDashboardSimpleJwtAuthSelector WithHeaderName(string headerName)
    {
        this.config.HeaderName = headerName;
        return this;
    }

    public IJobMasterDashboardSimpleJwtAuthSelector WithScheme(string scheme)
    {
        this.config.Scheme = scheme;
        return this;
    }
}
