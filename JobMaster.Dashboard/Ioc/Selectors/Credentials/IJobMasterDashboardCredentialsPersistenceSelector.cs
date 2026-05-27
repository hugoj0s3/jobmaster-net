using System;
using JobMaster.Dashboard.Configurations;

namespace JobMaster.Dashboard.Ioc.Selectors.Credentials;

public interface IJobMasterDashboardCredentialsPersistenceSelector
{
    /// <summary>
    /// Sets the persistence mechanism.
    /// </summary>
    IJobMasterDashboardCredentialsPersistenceSelector SetPersistenceType(DashboardCredentialsPersistenceType type);

    /// <summary>
    /// Sets how long a stored credential remains valid by default.
    /// </summary>
    IJobMasterDashboardCredentialsPersistenceSelector WithDefaultCredentialsExpiry(TimeSpan expiry);

}
