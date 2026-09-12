using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal class JobMasterDashboardUserPasswordAuthSelector : IJobMasterDashboardUserPasswordAuthSelector
{
    private readonly UserPasswordAuthProviderConfig config;

    public JobMasterDashboardUserPasswordAuthSelector(UserPasswordAuthProviderConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardUserPasswordAuthSelector WithDisplayName(string displayName)
    {
        this.config.DisplayName = displayName;
        return this;
    }

    public IJobMasterDashboardUserPasswordAuthSelector WithUserHeaderName(string headerName)
    {
        this.config.UserHeaderName = headerName;
        return this;
    }

    public IJobMasterDashboardUserPasswordAuthSelector WithPasswordHeaderName(string headerName)
    {
        this.config.PasswordHeaderName = headerName;
        return this;
    }
}
