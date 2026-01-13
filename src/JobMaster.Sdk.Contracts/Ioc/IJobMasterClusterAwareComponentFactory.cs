using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Repositories.Agent;

namespace JobMaster.Sdk.Contracts.Ioc;

public interface IJobMasterClusterAwareComponentFactory
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