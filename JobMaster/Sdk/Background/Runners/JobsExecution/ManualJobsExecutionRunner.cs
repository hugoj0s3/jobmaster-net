using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal class ManualJobsExecutionRunner : BucketAwareRunner, IJobsExecutionRunner
{
    private DateTime lastOnBoardingRunAtUtc = DateTime.MinValue;
    private IJobsExecutionEngine? jobExecutionEngine;
    
    private readonly IWorkerClusterOperations clusterOperations;
    private readonly IMasterBucketsService masterBucketsService;
    public IJobsOnboardingSource JobsOnboardingSource { get; }

    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);
    
    private ManualJobsExecutionRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        IJobsOnboardingSource jobsOnboardingSource,
        IJobsExecutionEngine? jobExecutionEngine) : base(backgroundAgentWorker)
    {
        this.JobsOnboardingSource = jobsOnboardingSource ?? throw new ArgumentNullException(nameof(jobsOnboardingSource));
        this.jobExecutionEngine = jobExecutionEngine;
        this.clusterOperations = backgroundAgentWorker.WorkerClusterOperations;
        this.masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
    }

    internal static ManualJobsExecutionRunner Create(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        IJobsOnboardingSource jobsOnboardingSource,
        IJobsExecutionEngine? jobExecutionEngine = null)
    {
        return new ManualJobsExecutionRunner(
            backgroundAgentWorker,
            jobsOnboardingSource,
            jobExecutionEngine);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        
        if (string.IsNullOrEmpty(BucketId))
        {
            return await Task.FromResult(OnTickResult.Skipped(this));
        }

        if (jobExecutionEngine is null)
        {
            jobExecutionEngine = this.BackgroundAgentWorker.GetOrCreateEngine(Priority, BucketId!);
        }
        
        await jobExecutionEngine.PulseAsync();
        
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
        
        var countAvailability = jobExecutionEngine!.CountOnBoardingAvailability();
        if (countAvailability == 0)
        {
            return;
        }

        if (countAvailability > this.BackgroundAgentWorker.BucketBufferSize)
        {
            countAvailability = this.BackgroundAgentWorker.BucketBufferSize;
        }
        
        var jobs = 
            await JobsOnboardingSource.PullAsync(
                countAvailability, 
                DateTime.UtcNow.Add(BackgroundAgentWorker.BucketBufferLeadTime));
        
        // Perform queue maintenance (abort timeouts, start queued) and decide if we should skip
        foreach (var job in jobs)
        {
            var result = await jobExecutionEngine.TryOnBoardingJobAsync(job, forceIfNoCapacity: true);
            logger.Debug($"JobId {job.Id} OnBoardingResult {result} ", JobMasterLogSubjectType.Job, job.Id);
            if (result == OnBoardingResult.Accepted)
            {
                continue;
            }

            if (result == OnBoardingResult.TooEarly)
            {
                job.MarkAsHeldOnMaster();
                await clusterOperations.ExecWithRetryAsync(async (o) => await o.UpsertAsync(job));
                logger.Warn($"JobId {job.Id} TooEarly {job.NextPlanExecutionAt:O} now {DateTime.UtcNow:O}", JobMasterLogSubjectType.Job, job.Id);
                continue;
            }
            
            if (result == OnBoardingResult.MovedToMaster)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
                continue;
            }
            
            logger.Error($"Unexpected OnBoardingResult. JobId {job.Id} OnBoardingResult {result}", JobMasterLogSubjectType.Job, job.Id);
            job.MarkAsHeldOnMaster();
            await clusterOperations.ExecWithRetryAsync(async (o) => await o.UpsertAsync(job));
        }
    }

    public JobMasterPriority Priority { get; private set; }
    
    public override async Task OnStopAsync()
    {
        if (jobExecutionEngine is null)
        {
            return;
        }
        
        await jobExecutionEngine.FlushToMasterAsync();
    }
    
    public void DefineBucketId(string bucketId, JobMasterPriority priority)
    {
        this.BucketId = bucketId;
        this.Priority = priority;
        
        logger.Debug($"Bucket defined. bucketId {bucketId}, priority {priority}", JobMasterLogSubjectType.Bucket, bucketId);
    }
}
