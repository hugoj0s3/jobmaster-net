using System;
using JobMaster.Dashboard.AuthRetention;
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

    /// <summary>
    /// Registers a custom <see cref="IJobMasterAuthRetentionStorage"/> implementation, taking precedence
    /// over the built-in implementation matching <see cref="SetAuthRetentionType"/>.
    /// </summary>
    IJobMasterDashboardAuthRetentionSelector UseCustom<T>() where T : class, IJobMasterAuthRetentionStorage;
}
