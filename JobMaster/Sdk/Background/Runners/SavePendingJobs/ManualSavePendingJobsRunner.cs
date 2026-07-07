using JobMaster.Abstractions.Models;
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
/// Processes <c>PendingSave</c> jobs queued for a specific bucket.
/// Must be activated via <see cref="DefineBucketId"/> before ticking; skips immediately
/// if no bucket ID is set or the bucket is not in an Active or Completing state.
/// For each pulled job, delegates to <see cref="JobSavePendingOperation"/>:
/// jobs whose <c>NextPlanExecutionAt</c> is beyond the transient threshold are held on master
/// directly; all others are routed to the best available bucket via the dispatcher.
/// On per-job failure the job is re-queued and a consecutive-failure backoff (10–60 s) is applied.
/// Runs every <see cref="SucceedInterval"/> while the bucket is healthy.
/// </summary>
internal class ManualSavePendingJobsRunner : BucketAwareRunner, ISavePendingJobsRunner
{
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IMasterClusterConfigurationService MasterClusterConfigurationService;
    
    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);

    public override TimeSpan SucceedInterval => interval;

    private int failedSavedCountConsecutive = 0;
    
    private JobSavePendingOperation? savePendingOperation;

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
            savePendingOperation = new JobSavePendingOperation(BackgroundAgentWorker, BucketId!);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
        {
            return OnTickResult.Skipped(this);
        }
        
        var jobs = await agentJobsDispatcherService
            .PullSavePendingJobsAsync(bucket.AgentConnectionId, BucketId!, BackgroundAgentWorker.BucketBufferSize);

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
                    logger.Error("Failed to save job", JobMasterLogCategory.Job, job.Id, exception: e);
                    // If re-queueing fails, we catch it so the loop continues for other jobs.
                    try 
                    {
                        job.AssignToBucket(bucket);
                        await agentJobsDispatcherService.AddSavePendingJobAsync(job);
                        pendingTracker.TryRemove(job.Id, out _);
                    }
                    catch (Exception e2)
                    { 
                        logger.Critical($"Failed to add job to queue. Data: {job.ToLogSummary()}", JobMasterLogCategory.Job, job.Id, exception: e2);
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
                    logger.Critical($"Failed to add job to queue. jobId: {job.Id} Data: {job.ToLogSummary()}");
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
