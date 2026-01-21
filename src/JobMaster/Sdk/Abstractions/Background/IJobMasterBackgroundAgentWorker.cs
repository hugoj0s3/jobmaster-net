using System.ComponentModel;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobMasterBackgroundAgentWorker
{
    AgentConnectionId AgentConnectionId { get; }
    string AgentWorkerId { get; }
    string WorkerName { get; }
    string? WorkerLane { get; }
    string AgentRepositoryTypeId { get; }
    IReadOnlyDictionary<JobMasterPriority, int> BucketQty { get; }
    JobMasterAgentConnectionConfig JobMasterAgentConnectionConfig { get; }
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
    IJobMasterRuntime? Runtime { get; }
    int BatchSize { get; }
    AgentWorkerMode Mode { get; }
    Task StartAsync();
    Task StopImmediatelyAsync();
    void RequestStop();
    
    bool StopRequested { get; }
    
    bool StopImmediatelyRequested { get; }
    
    DateTime? StopRequestedAt { get; }
    
    TimeSpan? StopGracePeriod { get; }
    
    CancellationTokenSource CancellationTokenSource { get; }
    
    IBucketRunnersFactory BucketRunnersFactory { get; }
    
    IServiceProvider ServiceProvider { get; }
    
    SemaphoreSlim BucketAwareSemaphoreSlim { get; } 
    SemaphoreSlim MainSemaphoreSlim { get; }

    T GetClusterAwareService<T>() where T : class, IJobMasterClusterAwareService;
    T GetClusterAwareRepository<T>() where T : class, IJobMasterClusterAwareMasterRepository;
    T GetClusterAwareComponent<T>() where T : class, IJobMasterClusterAwareComponent;
    
    double ParallelismFactor { get; }
    
    IWorkerClusterOperations WorkerClusterOperations { get; }
    
    IList<IJobMasterRunner> Runners { get; }
    
    bool IsOnWarmUpTime();
    IJobsExecutionEngine? GetEngine(string bucketId);
    IJobsExecutionEngine  GetOrCreateEngine(JobMasterPriority priority, string bucketId);
    
    bool IsInitialized { get; }
}