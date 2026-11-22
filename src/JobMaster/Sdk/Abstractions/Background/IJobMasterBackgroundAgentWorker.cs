using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobMasterBackgroundAgentWorker
{
    string AgentWorkerId { get; }
    HostId HostId { get; }
    string? WorkerLane { get; }
    IReadOnlyDictionary<JobMasterPriority, int> BucketQty { get; }
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
    IJobMasterRuntime? Runtime { get; }
    int TransferBatchSize { get; }
    int BucketBufferSize { get; }
    TimeSpan BucketBufferLeadTime { get; }
    AgentWorkerMode Mode { get; }
    Task StartAsync();
    Task StopImmediatelyAsync();
    /// <summary>
    /// Graceful stop: takes no new jobs and runs the ones it has for <paramref name="period"/> (at least
    /// <see cref="JobMasterConstants.MinGracefulStopPeriod"/>; default
    /// <see cref="JobMasterConstants.DefaultGracefulStopPeriod"/>), then stops immediately.
    /// </summary>
    void RequestGracefulStop(TimeSpan? period = null);
    
    bool GracefulStopRequested { get; }
    
    bool StopImmediatelyRequested { get; }
    
    DateTime? GracefulStopRequestedAt { get; }
    
    TimeSpan? GracefulStopPeriod { get; }
    
    StopControl StopControl { get; }

    
    IBucketRunnersFactory BucketRunnersFactory { get; }
    
    IServiceProvider MainServiceProvider { get; }
    
    SemaphoreSlim BucketAwareSemaphoreSlim { get; } 
    SemaphoreSlim MainSemaphoreSlim { get; }

    T GetClusterAwareService<T>() where T : class, IJobMasterClusterAwareService;
    T GetClusterAwareRepository<T>() where T : class, IJobMasterClusterAwareMasterRepository;
    T GetClusterAwareComponent<T>() where T : class, IJobMasterClusterAwareComponent;
    
    double ParallelismFactor { get; }
    
    IWorkerClusterOperations WorkerClusterOperations { get; }
    
    RunnerRegistry Runners { get; }
    
    bool IsOnWarmUpTime();
    IJobsExecutionEngine? GetEngine(string bucketId);
    IJobsExecutionEngine  GetOrCreateEngine(StopControl runnerStopControl, JobMasterPriority priority, string bucketId);

    void RegisterRuntimeBucket(BucketModel bucket);

    bool IsInitialized { get; }
}