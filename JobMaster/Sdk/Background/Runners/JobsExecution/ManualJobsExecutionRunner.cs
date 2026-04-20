using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal class ManualJobsExecutionRunner : BucketAwareRunner, IJobsExecutionRunner
{
    private DateTime lastOnBoardingRunAtUtc = DateTime.MinValue;
    private IJobsExecutionEngine? jobExecutionEngine;
    
    private IWorkerClusterOperations clusterOperations = null!;
    private IMasterBucketsService masterBucketsService = null!;
    public IJobsOnboardingSource JobsOnboardingSource { get; private set; } = null!;

    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);
    
    private ManualJobsExecutionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
    }

    internal static ManualJobsExecutionRunner Create(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        IJobsExecutionEngine? jobExecutionEngine = null)
    {
        var runner = new ManualJobsExecutionRunner(backgroundAgentWorker);
        runner.clusterOperations = backgroundAgentWorker.WorkerClusterOperations;
        runner.masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        runner.jobExecutionEngine = jobExecutionEngine;
        
        return runner;
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
        
        var countAvailability = jobExecutionEngine!.OnBoardingControl.CountAvailability();
        
        if (countAvailability == 0)
        {
            return;
        }

        if (countAvailability > this.BackgroundAgentWorker.BucketBufferSize)
        {
            countAvailability = this.BackgroundAgentWorker.BucketBufferSize;
        }
        
        var jobs = 
            await JobsOnboardingSource.TakeAsync(
                countAvailability, 
                DateTime.UtcNow.Add(BackgroundAgentWorker.BucketBufferLeadTime));
        
        // Perform queue maintenance (abort timeouts, start queued) and decide if we should skip
        foreach (var job in jobs)
        {
            var result = await jobExecutionEngine.TryOnBoardingJobAsync(job);
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
            }
        }
    }

    public void DefineBucketId(string bucketId, BucketType bucketType, JobMasterPriority priority)
    {
        if (string.IsNullOrEmpty(bucketId))
        {
            throw new ArgumentNullException(nameof(bucketId));
        }

        if (!string.IsNullOrEmpty(BucketId))
        {
            throw new InvalidOperationException("BucketId is already defined.");
        }

        switch (bucketType)
        {
            case BucketType.Fallback:
            {
                var fallbackCapacity = this.BackgroundAgentWorker.BucketBufferSize;
                if (fallbackCapacity <= 0)
                {
                    fallbackCapacity = new WorkerDefinition().BucketBufferSize; // default bucket buffer size
                }
                
                this.JobsOnboardingSource = new FallbackBucketJobsOnboardingSource(fallbackCapacity);
                break;
            }
            default:
            {
                var agentJobsDispatcherService =
                    this.BackgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
                this.JobsOnboardingSource = new StandardBucketJobsOnboardingSource(agentJobsDispatcherService, BackgroundAgentWorker.AgentConnectionId, bucketId);
                break;
            }
        }
         
        BucketId = bucketId;
        Priority = priority;
    }

    public JobMasterPriority Priority { get; protected set; }
    
    public override async Task OnStopAsync()
    {
        if (jobExecutionEngine is null)
        {
            return;
        }
        
        await jobExecutionEngine.FlushToMasterAsync();
    }
}

