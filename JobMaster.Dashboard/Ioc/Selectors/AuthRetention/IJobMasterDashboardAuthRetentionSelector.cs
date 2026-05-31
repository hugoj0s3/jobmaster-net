using System;
using JobMaster.Dashboard.Configurations;

namespace JobMaster.Dashboard.Ioc.Selectors.AuthRetention;

public interface IJobMasterDashboardAuthRetentionSelector
{
    /// <summary>
    /// Sets the persistence mechanism.
    /// </summary>
    IJobMasterDashboardAuthRetentionSelector SetAuthRetentionType(DashboardAuthRetentionType type);

    /// <summary>
    /// Sets how long a stored credential remains valid by default.
    /// </summary>
    IJobMasterDashboardAuthRetentionSelector WithDefaultCredentialsExpiry(TimeSpan expiry);
}
