using JobMaster.RavenDb.Agents;
using JobMaster.RavenDb.Connections;
using JobMaster.RavenDb.Master;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents.Operations.Expiration;
using JobMasterConstants = JobMaster.Sdk.Abstractions.JobMasterConstants;

namespace JobMaster.RavenDb;

internal sealed class RavenDbJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    private const int DefaultDbOperationThrottleLimitForCluster = 50;
    private const int DefaultDbOperationThrottleLimitForAgent = 25;

    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        return Task.FromResult<IList<string>>(new List<string>());
    }

    public async Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
        var allClusterConfigs = JobMasterClusterConnectionConfig.GetAllConfigs()
            .Where(c => c.RepositoryTypeId == RavenDbRepositoryConstants.RepositoryTypeId)
            .ToList();

        foreach (var clusterConfig in allClusterConfigs)
        {
            if (!clusterConfig.RuntimeDbOperationLimit.HasValue)
            {
                clusterConfig.SetRuntimeDbOperationLimit(DefaultDbOperationThrottleLimitForCluster);
            }

            // Forces the lazy fallback-connection registration now, before allAgentConfigs is built below.
            // JobMasterRuntime.StartAsync only creates it later (after OnBeforeStartAsync already ran), so
            // without this the fallback connection's Message collection would never get its static index
            // deployed here, and PullMessagesAsync/DestroyBucketAsync would throw IndexDoesNotExistException
            // the first time a fallback bucket is actually used. Standalone needs no equivalent call --
            // ClusterConfigBuilder already registers it eagerly at config-build time.
            clusterConfig.TryGetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);
        }

        var allAgentConfigs = JobMasterClusterConnectionConfig.GetAllConfigs()
            .SelectMany(c => c.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == RavenDbRepositoryConstants.RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in allAgentConfigs)
        {
            if (!agentConfig.RuntimeDbOperationLimit.HasValue)
            {
                agentConfig.SetRuntimeDbOperationLimit(DefaultDbOperationThrottleLimitForAgent);
            }
        }

        // The Message collection lives in each agent connection's own database (never the master's,
        // except for a fallback/standalone connection reusing the master's own connection string --
        // now guaranteed to already be in GetAllAgentConnectionConfigs() by the force-registration above).
        // Deployed unconditionally, not gated behind an opt-in like document expiration -- this only
        // reduces indexing overhead, it doesn't change any behavior a user would need to opt into.
        foreach (var agentConfig in allAgentConfigs)
        {
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(agentConfig.ClusterId);
            var storeManager = factory.ClusterServiceProvider.GetRequiredService<IRavenDbDocumentStoreManager>();
            var store = storeManager.GetOrCreateStore(agentConfig.ConnectionString, agentConfig.GetCertificate());
            var collectionName = agentConfig.GetCollectionPrefix() + RavenDbCollectionNames.Message;
            await RavenDbMessageIndexes.DeployAsync(store, collectionName);
        }

        // The Job collection lives in the master database -- one deployment per cluster is enough,
        // unlike Message's per-agent-connection loop above. Deployed unconditionally, same reasoning as
        // the Message index.
        foreach (var clusterConfig in allClusterConfigs)
        {
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var storeManager = factory.ClusterServiceProvider.GetRequiredService<IRavenDbDocumentStoreManager>();
            var store = storeManager.GetOrCreateStore(clusterConfig.ConnectionString, clusterConfig.GetCertificate());
            var collectionName = clusterConfig.GetCollectionPrefix() + RavenDbCollectionNames.Job;
            await RavenDbJobIndexes.DeployAsync(store, collectionName);
        }

        var expirationEnabledClusterConfigs = allClusterConfigs.Where(c => c.IsDocumentExpirationEnabled()).ToList();

        foreach (var clusterConfig in expirationEnabledClusterConfigs)
        {
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var storeManager = factory.ClusterServiceProvider.GetRequiredService<IRavenDbDocumentStoreManager>();
            var store = storeManager.GetOrCreateStore(clusterConfig.ConnectionString, clusterConfig.GetCertificate());

            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = (long)clusterConfig.GetDocumentExpirationFrequency().TotalSeconds,
            }));
        }
    }
}
