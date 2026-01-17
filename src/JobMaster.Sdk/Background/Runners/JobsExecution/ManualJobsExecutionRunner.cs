using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

public class ManualJobsExecutionRunner : BucketAwareRunner, IJobsExecutionRunner
{
    private IJobsExecutionEngine? JobExecutionEngine { get; set; }
    private IWorkerClusterOperations ClusterOperations { get; } = null!;
    
    private DateTime lastOnBoardingRunAtUtc = DateTime.MinValue;
    private readonly IMasterBucketsService masterBucketsService;
    private IAgentJobsDispatcherService AgentJobsDispatcherService { get; } = null!;

    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);
    
    public ManualJobsExecutionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        this.ClusterOperations = backgroundAgentWorker.WorkerClusterOperations;
        AgentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        this.masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        
        if (string.IsNullOrEmpty(BucketId))
        {
            return await Task.FromResult(OnTickResult.Skipped(this));
        }

        if (JobExecutionEngine is null)
        {
            JobExecutionEngine = this.BackgroundAgentWorker.GetOrCreateEngine(Priority, BucketId!);
        }
        
        await JobExecutionEngine.PulseAsync();
        
        var nowUtc = DateTime.UtcNow;
        if ((nowUtc - lastOnBoardingRunAtUtc) >= TimeSpan.FromSeconds(3))
        {
            await OnBoardingJobs(ct);
            lastOnBoardingRunAtUtc = nowUtc;
        }
        
        return await Task.FromResult(OnTickResult.Success(this));
    }

    private async Task OnBoardingJobs(CancellationToken ct)
    {
        var bucket = this.masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket is null)
        {
            return;
        }

        if (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing)
        {
            return;
        }
        
        var countAvailability = JobExecutionEngine!.OnBoardingControl.CountAvailability();
        
        if (countAvailability == 0)
        {
            return;
        }

        if (countAvailability > this.BackgroundAgentWorker.BatchSize)
        {
            countAvailability = this.BackgroundAgentWorker.BatchSize;
        }
        
        var jobs = 
            await AgentJobsDispatcherService.DequeueToProcessingAsync(
                BackgroundAgentWorker.AgentConnectionId, 
                BucketId!, 
                countAvailability, 
                DateTime.UtcNow.Add(JobMasterConstants.OnBoardingWindow));
        
        // Perform queue maintenance (abort timeouts, start queued) and decide if we should skip
        foreach (var job in jobs)
        {
            var result = await JobExecutionEngine.TryOnBoardingJobAsync(job);
            logger.Debug($"JobId {job.Id} OnBoardingResult {result} ", JobMasterLogSubjectType.Job, job.Id);
            if (result == OnBoardingResult.Accepted)
            {
                continue;
            }

            if (result == OnBoardingResult.TooEarly)
            {
                job.MarkAsHeldOnMaster();
                await ClusterOperations.ExecWithRetryAsync(async (o) => await o.UpsertAsync(job));
                logger.Warn($"JobId {job.Id} TooEarly {job.ScheduledAt:O} now {DateTime.UtcNow:O}", JobMasterLogSubjectType.Job, job.Id);
                continue;
            }
            
            if (result == OnBoardingResult.MovedToMaster)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }
        }
    }
    
    public void DefineBucketId(string bucketId, JobMasterPriority priority)
    {
        if (string.IsNullOrEmpty(bucketId))
        {
            throw new ArgumentNullException(nameof(bucketId));
        }

        if (!string.IsNullOrEmpty(BucketId))
        {
            throw new InvalidOperationException("BucketId is already defined.");
        }
        
        BucketId = bucketId;
        Priority = priority;
    }
    
    public JobMasterPriority Priority { get; protected set; }
    
    public override async Task OnStopAsync()
    {
        if (JobExecutionEngine is null)
        {
            return;
        }
        
        await JobExecutionEngine.FlushToMasterAsync();
    }
}

