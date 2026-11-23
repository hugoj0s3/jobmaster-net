using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;
using static JobMaster.Sdk.Ioc.ClusterServiceKeys;

namespace JobMaster.Sdk.Ioc;

internal class JobMasterClusterAwareComponentFactory : IJobMasterClusterAwareComponentFactory
{
    public string ClusterId { get; }
    
    public IServiceProvider ClusterServiceProvider { get; }
    
    public JobMasterClusterAwareComponentFactory(string clusterId, IServiceProvider serviceProvider)
    {
        ClusterServiceProvider = serviceProvider;
        this.ClusterId = clusterId;
    }
    
    public TService GetComponent<TService>() 
        where TService : class, IJobMasterClusterAwareComponent
    {
        return ClusterServiceProvider.GetRequiredService<TService>();
    }

    public TService GetService<TService>() where TService : class, IJobMasterClusterAwareService
    {
        return ClusterServiceProvider.GetRequiredService<TService>();
    }
    
    public TMasterRepo GetMasterRepository<TMasterRepo>()
        where TMasterRepo : class, IJobMasterClusterAwareMasterRepository
    {
        return ClusterServiceProvider.GetRequiredService<TMasterRepo>();
    }

    public IAgentJobsDispatcherRepository GetRepositoryDispatcher(string agentRepoTypeId)
    {
        var clusterConnConfig = ClusterServiceProvider.GetRequiredService<JobMasterClusterConnectionConfig>();
        var logger = ClusterServiceProvider.GetRequiredService<IJobMasterLogger>();

        if (!RepoTypeAndFingerprintUtil.IsHybrid(agentRepoTypeId))
        {
            // One store serves both roles; the saving and processing keys resolve the same type.
            return new SingleAgentJobsDispatcherRepository(
                clusterConnConfig,
                agentRepoTypeId,
                ClusterServiceProvider.GetAgentRawMessagesSavingRepository(agentRepoTypeId),
                logger);
        }

        var (transport, execution) = RepoTypeAndFingerprintUtil.TryParseHybridRepoType(agentRepoTypeId)
            ?? throw new InvalidOperationException($"Invalid hybrid repo type '{agentRepoTypeId}'.");

        var savePendingRepo = ClusterServiceProvider.GetAgentRawMessagesSavingRepository(transport);
        var processingRepo = ClusterServiceProvider.GetAgentRawMessagesProcessingRepository(execution);

        return new HybridJobsDispatcherRepository(
            clusterConnConfig,
            agentRepoTypeId,
            savePendingRepo,
            processingRepo,
            logger);
    }

    public IAgentFingerprintResolver GetFingerprintResolver(string agentRepoTypeId)
    {
        if (!RepoTypeAndFingerprintUtil.IsHybrid(agentRepoTypeId))
        {
            return new SingleAgentFingerprintResolver(
                agentRepoTypeId,
                ClusterServiceProvider.GetAgentRawFingerprintResolver(agentRepoTypeId));
        }

        var (transport, execution) = RepoTypeAndFingerprintUtil.TryParseHybridRepoType(agentRepoTypeId)
            ?? throw new InvalidOperationException($"Invalid hybrid repo type '{agentRepoTypeId}'.");

        return new HybridAgentFingerprintResolver(
            agentRepoTypeId,
            ClusterServiceProvider.GetAgentRawFingerprintResolver(transport),
            ClusterServiceProvider.GetAgentRawFingerprintResolver(execution));
    }

    public TBucketAwareRunner GetBucketAwareRunner<TBucketAwareRunner>(
        string agentRepoTypeId, 
        IJobMasterBackgroundAgentWorker backgroundWorker) 
        where TBucketAwareRunner : class, IBucketAwareRunner
    {
        var factoryDelegate = ClusterServiceProvider
            .GetRequiredKeyedService<Func<IJobMasterBackgroundAgentWorker, TBucketAwareRunner>>(
                GetAgentJobsDispatcherKey(agentRepoTypeId)
            );

        // 2. Invoke the delegate, passing the runtime worker instance
        return factoryDelegate(backgroundWorker);
    }
}
