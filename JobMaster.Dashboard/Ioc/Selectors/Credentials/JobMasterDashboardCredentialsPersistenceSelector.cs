using System;
using JobMaster.Dashboard.Configurations;

namespace JobMaster.Dashboard.Ioc.Selectors.Credentials;

internal class JobMasterDashboardCredentialsPersistenceSelector : IJobMasterDashboardCredentialsPersistenceSelector
{
    private readonly DashboardCredentialsPersistenceConfig config;

    public JobMasterDashboardCredentialsPersistenceSelector(DashboardCredentialsPersistenceConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardCredentialsPersistenceSelector SetPersistenceType(DashboardCredentialsPersistenceType type)
    {
        this.config.PersistenceType = type;
        return this;
    }

    public IJobMasterDashboardCredentialsPersistenceSelector WithDefaultCredentialsExpiry(TimeSpan expiry)
    {
        this.config.DefaultCredentialsExpiry = expiry;
        return this;
    }

}
