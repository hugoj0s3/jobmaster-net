using System;
using JobMaster.Dashboard.Configurations;

namespace JobMaster.Dashboard.Ioc.Selectors.AuthRetention;

internal class JobMasterDashboardAuthRetentionSelector : IJobMasterDashboardAuthRetentionSelector
{
    private readonly DashboardAuthRetentionConfig config;

    public JobMasterDashboardAuthRetentionSelector(DashboardAuthRetentionConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardAuthRetentionSelector SetAuthRetentionType(DashboardAuthRetentionType type)
    {
        this.config.AuthRetentionType = type;
        return this;
    }

    public IJobMasterDashboardAuthRetentionSelector WithDefaultCredentialsExpiry(TimeSpan expiry)
    {
        this.config.DefaultCredentialsExpiry = expiry;
        return this;
    }
}
