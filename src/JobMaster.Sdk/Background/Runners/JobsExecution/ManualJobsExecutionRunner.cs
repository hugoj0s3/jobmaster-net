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
    private IJobsExecutionEngine? jobExecutionEngine { get; set; }
    private IWorkerClusterOperations ClusterOperations { get; } = null!;
    
    private DateTime lastOnBoardingRunAtUtc = DateTime.MinValue;
    private IAgentJobsDispatcherService AgentJobsDispatcherService { get; } = null!;

    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);
    
    public ManualJobsExecutionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        this.ClusterOperations = backgroundAgentWorker.WorkerClusterOperations;
        AgentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
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
        var countAvailability = jobExecutionEngine!.OnBoardingControl.CountAvailability();
        
        if (countAvailability == 0)
        {
            return;
        }

        if (countAvailability > this.BackgroundAgentWorker.BatchSize)
        {
            countAvailability = this.BackgroundAgentWorker.BatchSize;
        }
        
        var jobs = 
            await AgentJobsDispatcherService.DequeueToProcessingAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!, countAvailability, DateTime.UtcNow.Add(JobMasterConstants.OnBoardingWindow));
        
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
                await ClusterOperations.ExecWithRetryAsync(async (o) => await o.UpsertAsync(job));
                continue;
            }
            
            if (result == OnBoardingResult.MovedToMaster)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }
        }
    }

    /// <summary>
    /// Configures the runner for a specific bucket and priority, setting up appropriate resource allocation.
    /// </summary>
    /// <param name="bucketId">The unique identifier of the bucket this runner will process</param>
    /// <param name="priority">The job priority level that determines resource allocation</param>
    /// <exception cref="ArgumentNullException">Thrown when bucketId is null or empty</exception>
    /// <exception cref="InvalidOperationException">Thrown when BucketId is already defined</exception>
    /// <remarks>
    /// <para>
    /// This method must be called before the runner can process jobs. It configures:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Task container capacity based on priority level</description></item>
    /// <item><description>Semaphore limits for concurrent job execution</description></item>
    /// <item><description>Priority-specific processing characteristics</description></item>
    /// </list>
    /// <para>
    /// Resource allocation by priority:
    /// - VeryLow: 3 tasks, 1 concurrent execution
    /// - Low: 3 tasks, 2 concurrent executions
    /// - Medium: 4 tasks, 3 concurrent executions
    /// - High: 5 tasks, 4 concurrent executions
    /// - Critical: 8 tasks, 5 concurrent executions
    /// </para>
    /// </remarks>
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
        if (jobExecutionEngine is null)
        {
            return;
        }
        
        await jobExecutionEngine.FlushToMasterAsync();
    }
}

