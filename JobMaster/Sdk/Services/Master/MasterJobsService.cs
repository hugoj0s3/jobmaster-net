using System.Diagnostics;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;

internal class MasterJobsService : JobMasterClusterAwareComponent, IMasterJobsService
{
    private IMasterJobsRepository masterJobsRepository = null!;
    private IJobMasterLogger logger = null!;
    private readonly IKnownExceptionIdentifier exceptionIdentifier;
    private OperationThrottler operationThrottler;
    
    // Serializes AcquireAndFetchAsync/BulkUpdateAsync (when useAcquireThrottler is set) to 1-at-a-time --
    // this is a per-cluster singleton (see ClusterConfigBuilder's AddSingleton registration), so this
    // throttler is shared by every coordinator instance in the cluster, not just one. Keeps one
    // coordinator's bulk-update phase from overlapping with another's acquire; the general,
    // much-higher-capacity operationThrottler would otherwise let those run fully concurrently.
    private OperationThrottler acquireOperationThrottler = new (1, 5000);

    public MasterJobsService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterJobsRepository masterJobsRepository,
        IJobMasterLogger logger,
        IKnownExceptionIdentifier exceptionIdentifier) : base(clusterConnectionConfig)
    {
        this.masterJobsRepository = masterJobsRepository;
        this.logger = logger;
        this.exceptionIdentifier = exceptionIdentifier;
        this.operationThrottler = OperationThrottlerSettingsFactory.GetMasterThrottler(clusterConnectionConfig.ClusterId);
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        await operationThrottler.ExecAsync(async () =>
        {
            try
            {
                await masterJobsRepository.AddAsync(jobRaw);
            }
            catch (JobMasterDuplicationException) 
            {
                throw;
            }
            catch (Exception ex)
            {
                if (await this.masterJobsRepository.ExistsAsync(jobRaw.Id))
                {
                    throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
                }
                
                throw;
            }
        });
    }

    public void Add(JobRawModel jobRaw)
    {
        operationThrottler.Exec(() =>
        {
            try
            {
                masterJobsRepository.Add(jobRaw);
            }
            catch (Exception ex)
            {
                if (this.masterJobsRepository.Exists(jobRaw.Id))
                {
                    throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
                }
                
                throw;
            }
        });
    }

    public async Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null, OperationThrottler? throttler = null)
    {
        ValidateJobExecutionOutcome(jobRaw, addJobExecution);

        try
        {
            await (throttler ?? operationThrottler).ExecAsync(() => masterJobsRepository.UpdateAsync(jobRaw, addJobExecution));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogCategory.Job, jobRaw.Id, e);
            throw;
        }
    }

    public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null, OperationThrottler? throttler = null)
    {
        ValidateJobExecutionOutcome(jobRaw, addJobExecution);

        try
        {
            (throttler ?? operationThrottler).Exec(() => masterJobsRepository.Update(jobRaw, addJobExecution));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogCategory.Job, jobRaw.Id, e);
            throw;
        }
    }

    public Task AddJobExecutionAsync(JobExecution jobExecution)
    {
        jobExecution.EnsureFinalized();
        return operationThrottler.ExecAsync(() => masterJobsRepository.AddJobExecutionAsync(jobExecution));
    }

    public Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.QueryJobExecutionsAsync(jobId));
    }

    public async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, DateTime expiresAtUtc)
    {
        var partitionLockId = JobMasterRandomUtil.NewGuid7();
        try
        {
            return await acquireOperationThrottler.ExecAsync(() => masterJobsRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc));
        }
        catch (Exception ex) when (exceptionIdentifier.Identify(ex) == JobMasterKnownExceptionId.Deadlock)
        {
            // A deadlock here means the claim transaction was rolled back before it committed any
            // row locks, so no job was actually acquired under partitionLockId -- functionally
            // identical to a tick that simply found nothing to claim. Returning empty instead of
            // propagating lets the caller's normal poll-and-retry-next-tick behavior handle it,
            // rather than surfacing an exception for an outcome the system already treats as routine.
            return Array.Empty<JobRawModel>();
        }
    }

    public IList<JobRawModel> Query(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterJobsRepository.Query(queryCriteria));
    }

    public Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.QueryAsync(queryCriteria));
    }
    
    public long Count(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterJobsRepository.Count(queryCriteria));
    }

    public Task<JobProbeResult> ProbeForAcquireAsync(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.ProbeForAcquireAsync(queryCriteria));
    }

    public JobRawModel? Get(Guid jobId)
    {
        return operationThrottler.Exec(() => masterJobsRepository.Get(jobId));
    }

    public Task<JobRawModel?> GetAsync(Guid jobId)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.GetAsync(jobId));
    }

    public bool CheckVersion(Guid jobId, string? expectedVersion)
    {
        if (expectedVersion is null)
        {
            return false;
        }
        
        var job = operationThrottler.Exec(() => masterJobsRepository.Get(jobId));
        if (job == null)
        {
            return false;
        }
        
        return job.Version == expectedVersion;
    }
    
    public async Task<bool> CheckVersionAsync(Guid jobId, string? expectedVersion)
    {
        if (expectedVersion is null)
        {
            return false;
        }
        
        var job = await operationThrottler.ExecAsync(() => masterJobsRepository.GetAsync(jobId));
        if (job == null)
        {
            return false;
        }
        
        return job.Version == expectedVersion;
    }

    public OperationThrottler AcquireThrottler => acquireOperationThrottler;

    public async Task BulkUpdateAsync(BulkJobUpdateRequest request, OperationThrottler? throttler = null)
    {
        if (request.JobIds.Count == 0 || request.Properties.Count == 0) return;
        await (throttler ?? operationThrottler).ExecAsync(() => masterJobsRepository.BulkUpdateAsync(request));
    }

    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs, OperationThrottler? throttler = null)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();
        return await (throttler ?? operationThrottler).ExecAsync(() => masterJobsRepository.BulkUpdateAsync(jobs));
    }
    
    
    private static void ValidateJobExecutionOutcome(JobRawModel jobRaw, JobExecution? addJobExecution)
    {
        if (jobRaw.Status == JobMasterJobStatus.Succeeded &&
            addJobExecution != null &&
            addJobExecution.Outcome != JobExecutionOutcomeStatus.Succeeded)
        {
            throw new ArgumentException("Job execution outcome must be succeeded when job status is succeeded.");
        }
        
        
        if (jobRaw.Status == JobMasterJobStatus.Failed &&
            addJobExecution != null &&
            addJobExecution.Outcome != JobExecutionOutcomeStatus.Failed)
        {
            throw new ArgumentException("Job execution outcome must be failed when job status is failed.");
        }
        
        // A Failed execution outcome is valid alongside two job statuses, not just Failed:
        // TryRetry() (JobRawModel.cs) intentionally sets Status back to OnMaster -- not
        // Failed -- when retries remain, so the job is picked up fresh from the master queue.
        // That's a per-attempt failure on a job that isn't done retrying yet, not a
        // contradiction; only a Failed outcome paired with some other, genuinely inconsistent
        // status (e.g. Succeeded, InBucket) should be rejected here.
        if (addJobExecution != null &&
            addJobExecution.Outcome != JobExecutionOutcomeStatus.Succeeded
            && jobRaw.Status != JobMasterJobStatus.Failed
            && jobRaw.Status != JobMasterJobStatus.OnMaster)
        {
            throw new ArgumentException("Job execution outcome must be succeeded, or the job must be Failed or OnMaster (retry pending), when the execution outcome is not succeeded.");
        }
        
        addJobExecution?.EnsureFinalized();
    }
}
