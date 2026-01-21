using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.SavePendingJobs;

/// <summary>
/// Handles the persistence of pending job changes to ensure data consistency and durability.
/// This runner is essential for maintaining job state integrity by saving pending modifications to persistent storage.
/// </summary>
/// <remarks>
/// <para>
/// The SaveJobsRunner performs the following critical data persistence functions:
/// </para>
/// <list type="bullet">
/// <item><description>Dequeues pending job save operations from the dispatcher service</description></item>
/// <item><description>Processes job state changes that need to be persisted to storage</description></item>
/// <item><description>Applies transient threshold logic to determine save timing and priorities</description></item>
/// <item><description>Ensures job modifications are durably stored for system reliability</description></item>
/// <item><description>Manages save operation batching to optimize database performance</description></item>
/// </list>
/// <para>
/// Performance characteristics:
/// - Uses 250ms intervals for responsive job state persistence
/// - Implements adaptive delays when no pending saves are available
/// - Processes jobs in configurable batch sizes to balance throughput and memory usage
/// - Applies transient threshold logic to prioritize time-sensitive job saves
/// </para>
/// <para>
/// This runner is bucket-specific and works in coordination with job execution runners
/// to ensure that job state changes are promptly and reliably persisted to storage,
/// preventing data loss during system failures or restarts.
/// </para>
/// </remarks>
internal class ManualSavePendingJobsRunner : BucketAwareRunner, ISavePendingJobsRunner
{
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IMasterClusterConfigurationService MasterClusterConfigurationService;
    
    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);

    public override TimeSpan SucceedInterval => interval;

    private int failedSavedCountConsecutive = 0;
    
    private SavePendingOperation? savePendingOperation;

    public ManualSavePendingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        MasterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }

        if (savePendingOperation is null)
        {
            savePendingOperation = new SavePendingOperation(BackgroundAgentWorker, BucketId!);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
        {
            return OnTickResult.Skipped(this);
        }
        
        var jobs = await agentJobsDispatcherService.DequeueSavePendingJobsAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!, BackgroundAgentWorker.BatchSize);

        if (jobs.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMilliseconds(interval.TotalMilliseconds * 5));
        }
        
        var configuration = MasterClusterConfigurationService.Get();
        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);
        
        DateTime cutOffDate = DateTime.UtcNow.Add(transientThreshold);
        bool hasFailed = false;
        
        var pendingTracker = new System.Collections.Concurrent.ConcurrentDictionary<Guid, JobRawModel>(
            jobs.ToDictionary(x => x.Id, x => x)
        );
        
        using var batchTimeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(2.5));
        var parallelOptions = new ParallelOptions 
        { 
            MaxDegreeOfParallelism = 5,
            CancellationToken = batchTimeoutCts.Token 
        };
        
        try
        {
            await JobMasterParallelUtil.ForEachAsync(jobs, parallelOptions, async (job, token) =>
            {
                try
                {
                    await savePendingOperation.AddSavePendingJobAsync(job, cutOffDate);
                    pendingTracker.TryRemove(job.Id, out _);
                }
                catch (Exception e)
                {
                    logger.Error("Failed to save job", JobMasterLogSubjectType.Job, job.Id, exception: e);
                    // If re-queueing fails, we catch it so the loop continues for other jobs.
                    try 
                    {
                        await agentJobsDispatcherService.AddSavePendingJobAsync(job);
                        pendingTracker.TryRemove(job.Id, out _);
                    }
                    catch (Exception e2)
                    { 
                        logger.Critical($"Failed to add job to queue. Data: {InternalJobMasterSerializer.Serialize(job)}", JobMasterLogSubjectType.Job, job.Id, exception: e2);
                    }
                
                    hasFailed = true;
                }
            });
        }
        catch (OperationCanceledException)
        {
            foreach(var job in pendingTracker.Values) 
            {
                try 
                {
                    await agentJobsDispatcherService.AddSavePendingJobAsync(job);
                }
                catch 
                { 
                    // CRITICAL: Job is lost in memory. Log this if logger available. 
                }
            }
        }
        
        if (hasFailed)
        {
            failedSavedCountConsecutive++;
    
            // Cap at 60 seconds max wait
            var secondsToWait = Math.Min(60, 10 + failedSavedCountConsecutive * 5);
    
            return OnTickResult.Skipped(TimeSpan.FromSeconds(secondsToWait));
        }
        
        failedSavedCountConsecutive = 0;
        return OnTickResult.Success(this);
     }
    
    public void DefineBucketId(string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
            throw new ArgumentNullException(nameof(bucketId));
        
        if (!string.IsNullOrEmpty(BucketId))
            throw new InvalidOperationException("BucketId is already defined.");
        
        this.BucketId = bucketId;
    }
}