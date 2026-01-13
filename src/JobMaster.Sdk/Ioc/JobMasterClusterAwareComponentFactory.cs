using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using Microsoft.Extensions.DependencyInjection;
using static JobMaster.Sdk.Ioc.ClusterServiceKeys;

namespace JobMaster.Sdk.Ioc;

public class JobMasterClusterAwareComponentFactory : IJobMasterClusterAwareComponentFactory
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
        return ClusterServiceProvider
            .GetRequiredKeyedService<IAgentJobsDispatcherRepository>(GetAgentJobsDispatcherKey(agentRepoTypeId));
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