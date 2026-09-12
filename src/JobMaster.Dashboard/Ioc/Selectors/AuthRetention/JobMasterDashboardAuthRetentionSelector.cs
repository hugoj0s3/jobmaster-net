using System;
using JobMaster.Dashboard.AuthRetention;
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

    public IJobMasterDashboardAuthRetentionSelector UseCustom<T>() where T : class, IJobMasterAuthRetentionStorage
    {
        this.config.AuthRetentionType = DashboardAuthRetentionType.Custom;
        this.config.CustomAuthRetentionStorageType = typeof(T);
        return this;
    }
}
