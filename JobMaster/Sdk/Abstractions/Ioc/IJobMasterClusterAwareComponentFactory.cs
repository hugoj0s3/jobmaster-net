using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Repositories.Agent;

namespace JobMaster.Sdk.Abstractions.Ioc;

internal interface IJobMasterClusterAwareComponentFactory
{
    string ClusterId { get; }
    IServiceProvider ClusterServiceProvider { get; }
    TComponent GetComponent<TComponent>() 
        where TComponent : class, IJobMasterClusterAwareComponent;
    TService GetService<TService>()
        where TService : class, IJobMasterClusterAwareService;
    
    TMasterRepo GetMasterRepository<TMasterRepo>()
        where TMasterRepo : class, IJobMasterClusterAwareMasterRepository;
    
    IAgentJobsDispatcherRepository GetRepositoryDispatcher(string agentRepoTypeId);

    public TBucketAwareRunner GetBucketAwareRunner<TBucketAwareRunner>(
        string agentRepoTypeId,
        IJobMasterBackgroundAgentWorker backgroundWorker)
        where TBucketAwareRunner : class, IBucketAwareRunner;
}