using System.Collections.ObjectModel;
using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background.Runners;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;
using JobMaster.Sdk.Background.Runners.CleanUpData;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Ioc.Definitions;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background;

public class JobMasterBackgroundAgentWorker : IDisposable, IJobMasterBackgroundAgentWorker
{
    public AgentConnectionId AgentConnectionId { get; private set; } = null!;
    public string AgentWorkerId { get; private set; } = string.Empty;

    public string WorkerName { get; private set; } = string.Empty;
    public string? WorkerLane { get; private set; }

    public string AgentRepositoryTypeId { get; private set; } = string.Empty;
    
    public bool SkipWarmUpTime { get; private set; }
    
    public IReadOnlyDictionary<JobMasterPriority, int> BucketQty { get; private set; } = null!;

    public JobMasterAgentConnectionConfig JobMasterAgentConnectionConfig { get; private set; } = null!;
    
    public JobMasterClusterConnectionConfig ClusterConnConfig { get; private set; } = null!;
    
    public IJobMasterRuntime? Runtime => JobMasterRuntimeSingleton.Instance;
    
    public bool StopRequested { get; internal set; }

    public int BatchSize { get; private set; } = 0;

    public AgentWorkerMode Mode { get; private set; } = AgentWorkerMode.Standalone;
    
    public CancellationTokenSource CancellationTokenSource { get; private set; } = new CancellationTokenSource();
    
    public IBucketRunnersFactory BucketRunnersFactory { get; private set; } = null!;
    
    public IServiceProvider ServiceProvider { get; private set; } = null!;
    
    public IList<IJobMasterRunner> Runners { get; private set; } = new List<IJobMasterRunner>();
    public bool IsOnWarmUpTime()
    {
        return SkipWarmUpTime == false && Runtime?.IsOnWarmUpTime() == true;
    }

    public IWorkerClusterOperations WorkerClusterOperations { get; private set; } = null!;
    
    public SemaphoreSlim BucketAwareSemaphoreSlim { get; private set; } = null!;
    public SemaphoreSlim MainSemaphoreSlim { get; private set; }  = new SemaphoreSlim(3);
    
    public bool StopImmediatelyRequested { get; internal set; } = false;
    
    public DateTime? StopRequestedAt { get; private set; }
    
    private readonly object mutex = new object();
    private JobMasterLockKeys lockKeys = null!;
    
    private List<BucketModel> BucketsCreatedOnRuntime = new List<BucketModel>();
    
    // Services
    private IMasterBucketsService masterBucketsService = null!;
    private IMasterAgentWorkersService masterAgentWorkersService = null!;
    
    private IJobMasterLogger logger = null!;
    
    private IJobsExecutionEngineFactory jobsExecutionEngineFactory = null!;

    internal JobMasterBackgroundAgentWorker()
    {
    }
    
    internal IJobMasterClusterAwareComponentFactory JobMasterClusterAwareAwareFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);

    public T GetClusterAwareService<T>() where T : class, IJobMasterClusterAwareService
    {
        return JobMasterClusterAwareAwareFactory.GetService<T>();
    }
    
    public T GetClusterAwareRepository<T>() where T : class, IJobMasterClusterAwareMasterRepository
    {
        return JobMasterClusterAwareAwareFactory.GetMasterRepository<T>();
    }
    
    public T GetClusterAwareComponent<T>() where T : class, IJobMasterClusterAwareComponent
    {
        return JobMasterClusterAwareAwareFactory.GetComponent<T>();
    }

    public static async Task<JobMasterBackgroundAgentWorker> CreateAsync(
        IServiceProvider serviceProvider,
        string clusterId,
        string agentConnName,
        string workerName,
        string? workerLane,
        AgentWorkerMode mode)
    {
        if (JobMasterRuntimeSingleton.Instance?.Started == true)
        {
            throw new InvalidOperationException("JobMasterRuntime is already started");
        }
        
        var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.First(x => x.ClusterId == clusterId);
        
        var workerDefinition = clusterDefinition.Workers.FirstOrDefault(x => x.AgentConnectionName == agentConnName && x.WorkerName == workerName);
        if (workerDefinition == null)
        {
            throw new ArgumentException($"Worker '{workerName}' not found in cluster configuration.", nameof(workerName));
        }
        
        if (string.IsNullOrEmpty(workerName))
        {
            throw new ArgumentException("Worker name cannot be empty");
        }
        
        var clusterConfig = JobMasterClusterConnectionConfig.Get(clusterId, includeInactive: true);
        if (clusterConfig == null)
        {
            throw new ArgumentException($"Cluster '{clusterId}' not found");
        }
        
        var agentConfig = clusterConfig.GetAgentConnectionConfig(agentConnName);
        if (agentConfig == null)
        {
            throw new ArgumentException($"Agent '{agentConnName}' not found in cluster configuration.", nameof(agentConnName));
        }
        
        var agentConnectionId = clusterConfig.GetAgentConnectionConfig(agentConnName).Id;

        var clusterAwareFactory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
        var masterAgentsService =
            clusterAwareFactory.GetComponent<IMasterAgentWorkersService>();
        
        
        var agentConnectionString = clusterConfig.GetAgentConnectionConfig(agentConnName);
        var workerId = await masterAgentsService.RegisterWorkerAsync(agentConnectionId, workerName.NotNull(), workerLane, mode);

        var qtyOfBuckets = workerDefinition.BucketQty.Sum(x => x.Value);
        var background = new JobMasterBackgroundAgentWorker()
        {
            AgentConnectionId = new AgentConnectionId(agentConnectionId),
            AgentWorkerId = workerId,
            JobMasterAgentConnectionConfig = agentConnectionString,
            AgentRepositoryTypeId = agentConnectionString.RepositoryTypeId,
            ClusterConnConfig = clusterConfig,
            ServiceProvider = serviceProvider,
            WorkerName = workerName,
            BucketQty = new ReadOnlyDictionary<JobMasterPriority, int>(workerDefinition.BucketQty),
            BatchSize = workerDefinition.BatchSize,
            BucketAwareSemaphoreSlim = new SemaphoreSlim(qtyOfBuckets),
            Mode = mode,
            WorkerLane = workerLane,
            SkipWarmUpTime = workerDefinition.SkipWarmUpTime,
        };
        background.lockKeys = new JobMasterLockKeys(clusterConfig.ClusterId);
        
        background.InitializeDependencies();

        return background;
    }

    private void InitializeDependencies()
    {
        masterBucketsService = this.GetClusterAwareService<IMasterBucketsService>();
        masterAgentWorkersService = this.GetClusterAwareService<IMasterAgentWorkersService>();
        BucketRunnersFactory = this.GetClusterAwareComponent<IBucketRunnersFactory>();
        WorkerClusterOperations = this.GetClusterAwareComponent<IWorkerClusterOperations>();
        
        logger = this.GetClusterAwareService<IJobMasterLogger>();
        
        jobsExecutionEngineFactory = new JobsExecutionEngineFactory();
    }

    public async Task StartAsync()
    {
        logger.Info("Starting JobMasterBackgroundAgentWorker", JobMasterLogSubjectType.AgentWorker, this.AgentWorkerId);
        var heartBeatRunner = new KeepAliveRunner(this);
        await heartBeatRunner.StartAsync();
        
        if (this.Mode == AgentWorkerMode.Standalone)
        {
            await LoadStandaloneRunnersAsync();    
        }
        
        if (this.Mode == AgentWorkerMode.Drain)
        {
            await LoadDrainJobsRunnerAsync(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
            await LoadDrainMaintenanceRunnersAsync();
        }
        
        if (this.Mode == AgentWorkerMode.Coordinator)
        {
            await LoadCoordinatorRunnersAsync();
        }
        
        if (this.Mode == AgentWorkerMode.Execution)
        {
            await LoadBucketsForExecution();
        }
        
        logger.Info("Started JobMasterBackgroundAgentWorker", JobMasterLogSubjectType.AgentWorker, this.AgentWorkerId);
    }
    
    private async Task LoadStandaloneRunnersAsync()
    {
        // Standalone must cover coordination, drain management, and execution buckets
        await LoadCoordinatorRunnersAsync();
        // Use longer intervals for drain-related runners in standalone
        await LoadDrainJobsRunnerAsync(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        await LoadBucketsForExecution();
    }


    private async Task LoadDrainJobsRunnerAsync(TimeSpan drainCoordinatorInterval, TimeSpan assignedLostInterval)
    {
        // Drain-specific pipeline coordination (no heartbeat here)
        var drainJobsRunnerCreatorRunner = new DrainRunnersCoordinator(this, drainCoordinatorInterval);
        await drainJobsRunnerCreatorRunner.StartAsync();
        
        var assignedLostBucketToDrainRunner = new AssignedLostBucketsRunner(this, succeedInterval: assignedLostInterval);
        await assignedLostBucketToDrainRunner.StartAsync();
    }

    private async Task LoadDrainMaintenanceRunnersAsync()
    {
        // Maintenance tasks useful during drain mode
        var markBucketAsLostRunner = new MarkBucketAsLostRunner(this, succeedInterval: TimeSpan.FromSeconds(30));
        await markBucketAsLostRunner.StartAsync();
        
        var destroyReadyToDeleteBucketsRunner = new DestroyReadyToDeleteBucketsRunner(this);
        await destroyReadyToDeleteBucketsRunner.StartAsync();
    }
    
    private async Task LoadCoordinatorRunnersAsync()
    {
        var assignHeldJobsRunner = new AssignJobsToBucketsRunner(this);
        await assignHeldJobsRunner.StartAsync();
        
        var processDeadlineTimeoutRunner = new HeldOnMasterDeadlineTimeoutJobsRunner(this);
        await processDeadlineTimeoutRunner.StartAsync();
        
        var workerStopCoordinatorRunner = new WorkerStopCoordinatorRunner(this);
        await workerStopCoordinatorRunner.StartAsync();
        
        var markBucketAsLostRunner = new MarkBucketAsLostRunner(this);
        await markBucketAsLostRunner.StartAsync();
        
        var destroyReadyToDeleteBucketsRunner = new DestroyReadyToDeleteBucketsRunner(this);
        await destroyReadyToDeleteBucketsRunner.StartAsync();
        
        var assignedLostBucketToDrainRunner = new AssignedLostBucketsRunner(this, succeedInterval: TimeSpan.FromMinutes(1));
        await assignedLostBucketToDrainRunner.StartAsync();
        
        var deadWorkerCleanupRunner = new DeadWorkerCleanupRunner(this);
        await deadWorkerCleanupRunner.StartAsync();
        
        var scheduleRecurringJobsRunner = new ScheduleRecurringJobsRunner(this);
        await scheduleRecurringJobsRunner.StartAsync();
        
        var deleteExpiredGenericRecordsRunner = new DeleteExpiredGenericRecordsRunner(this);
        await deleteExpiredGenericRecordsRunner.StartAsync();
        
        var deleteOldFinalJobsRunner = new DeleteOldFinalJobsRunner(this);
        await deleteOldFinalJobsRunner.StartAsync();
        
        var deleteOldInactiveRecurringSchedulesRunner = new DeleteOldInactiveRecurringSchedulesRunner(this);
        await deleteOldInactiveRecurringSchedulesRunner.StartAsync();
        
        var staticRecurringDefinitionsKeepAliveRunner = new StaticRecurringDefinitionsKeepAliveRunner(this);
        await staticRecurringDefinitionsKeepAliveRunner.StartAsync();
        
        var cancelJobsFromRecurScheduleInactiveOrCanceled = new CancelJobsFromRecurScheduleInactiveOrCanceledRunner(this);
        await cancelJobsFromRecurScheduleInactiveOrCanceled.StartAsync();
        
        var deleteOldLogsRunner = new DeleteOldLogsRunner(this);
        await deleteOldLogsRunner.StartAsync();
    }

    private async Task LoadBucketsForExecution()
    {
        var buckets = new List<BucketModel>();
        foreach (var item in this.BucketQty)
        {
            var bucketQty = item.Value;
            var bucketPriority = item.Key;
            var createdBuckets = await CreateBucketsAsync(bucketPriority, bucketQty);
            buckets.AddRange(createdBuckets);
            
            // Add to BucketsCreatedOnRuntime immediately so GetOrCreateEngine can validate
            foreach (var bucket in createdBuckets)
            {
                this.BucketsCreatedOnRuntime.Add(bucket);
            }
        }

        foreach (var bucketModel in buckets)
        {
            var saveJobsRunner = this.BucketRunnersFactory.NewSavePendingJobsRunner(this, AgentConnectionId);
            saveJobsRunner.DefineBucketId(bucketModel.Id);
            await saveJobsRunner.StartAsync();
        
            var jobsExecutionRunner = this.BucketRunnersFactory.NewJobsExecutionRunner(this, AgentConnectionId);
            jobsExecutionRunner.DefineBucketId(bucketModel.Id, bucketModel.Priority);
            await jobsExecutionRunner.StartAsync();
        
            var saveRecurringScheduleRunner = this.BucketRunnersFactory.NewSaveRecurringSchedulerRunner(this, AgentConnectionId);
            saveRecurringScheduleRunner.DefineBucketId(bucketModel.Id);
            await saveRecurringScheduleRunner.StartAsync();
        }
    }

    public async Task StopImmediatelyAsync()
    {
        foreach (var runner in new List<IJobMasterRunner>(this.Runners))
        {
            await runner.StopAsync();
        }
        
        lock (this.mutex)
        {
            if (this.StopImmediatelyRequested)
            {
                return;
            }

            this.StopImmediatelyRequested = true;
            
            foreach (var bucket in this.BucketsCreatedOnRuntime)
            {
                if (bucket.Status == BucketStatus.Lost) continue;
                
                WorkerClusterOperations.MarkBucketAsLost(bucket);
            }

            var buckets = this.masterBucketsService.QueryAllNoCache();
            foreach (var bucket in buckets)
            {
                if (bucket.AgentWorkerId != this.AgentWorkerId) continue;
                if (bucket.Status == BucketStatus.Lost) continue;
                
                WorkerClusterOperations.MarkBucketAsLost(bucket);
            }
        }
        
        await RunnerDelayUtil.DelayAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        await this.masterAgentWorkersService.DeleteWorkerAsync(this.AgentWorkerId);
    }

    public void RequestStop()
    {
        this.StopRequestedAt = DateTime.UtcNow;
        
        foreach (var bucket in this.BucketsCreatedOnRuntime)
        {
            if (bucket.Status == BucketStatus.Active)
            {
                bucket.MarkAsCompleting();
                masterBucketsService.Update(bucket);
            }
        }
        
        this.StopRequested = true;
    }

    public IJobsExecutionEngine? SafeGetOrCreateEngine(JobMasterPriority priority, string bucketId)
    {
        // Check if the bucket belongs to this worker
        var bucket = BucketsCreatedOnRuntime.FirstOrDefault(b => b.Id == bucketId);
        if (bucket is null)
        {
            return null;
        }
        
        // Use factory to get or create the engine
        return jobsExecutionEngineFactory.GetOrCreate(this, priority, bucketId);
    }

    public IJobsExecutionEngine? GetEngine(string bucketId)
    {
       return jobsExecutionEngineFactory.Get(bucketId);
    }

    public IJobsExecutionEngine GetOrCreateEngine(JobMasterPriority priority, string bucketId)
    {
        var bucket = BucketsCreatedOnRuntime.FirstOrDefault(b => b.Id == bucketId);
        if (bucket is null)
        {
            throw new ArgumentException("Bucket not found");
        }
        
        return jobsExecutionEngineFactory.GetOrCreate(this, priority, bucketId);
    }

    public void Dispose()
    {
        this.CancellationTokenSource.Dispose();
    }
    
    private async Task<IList<BucketModel>> CreateBucketsAsync(JobMasterPriority priority, int qty)
    {
        var buckets = new List<BucketModel>();
        for (int i = 0; i < qty; i++)
        {
            var bucket = await this.masterBucketsService.CreateAsync(AgentConnectionId, AgentWorkerId, priority);
            buckets.Add(bucket);

            await Task.Delay(JobMasterRandomUtil.GetInt(50, 100));
        }

        return buckets;
    }
}