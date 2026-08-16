using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents.Operations.Expiration;

namespace JobMaster.RavenDb;

/// <summary>
/// RavenDB provider runtime setup. Auto-discovered and invoked by <c>ClusterConfigBuilder</c> like every
/// other <see cref="IJobMasterRuntimeSetup"/> implementation -- no manual registration needed. Enables
/// RavenDB's native document-expiration background job on each configured database, which is disabled
/// by default and required for <see cref="JobMaster.RavenDb.Master.RavenDbMasterDistributedLockerRepository"/>'s
/// zombie-lock <c>@expires</c> backstop to actually delete anything. Database provisioning and index
/// deployment are not handled here.
/// </summary>
internal sealed class RavenDbJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        return Task.FromResult<IList<string>>(new List<string>());
    }

    public async Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
        var clusterConfigs = JobMasterClusterConnectionConfig.GetAllConfigs()
            .Where(cfg => cfg.RepositoryTypeId == RavenDbRepositoryConstants.RepositoryTypeId)
            .ToList();

        foreach (var clusterConfig in clusterConfigs)
        {
            await ConfigureExpirationAsync(clusterConfig.ClusterId, clusterConfig.ConnectionString);
        }

        var agentConfigs = JobMasterClusterConnectionConfig.GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == RavenDbRepositoryConstants.RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in agentConfigs)
        {
            await ConfigureExpirationAsync(agentConfig.ClusterId, agentConfig.ConnectionString);
        }
    }
    
    private static async Task ConfigureExpirationAsync(string clusterId, string connectionString)
    {
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
        var storeManager = factory.ClusterServiceProvider.GetRequiredService<IRavenDbDocumentStoreManager>();
        var store = storeManager.GetOrCreateStore(connectionString);
        
        await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
        {
            Disabled = false,
        }));
    }
}
