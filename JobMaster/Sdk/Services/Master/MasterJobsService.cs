using System.Diagnostics;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
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
    
    // Less concurrent for acquire operations is better for performance only 1 per 5000ms.
    private OperationThrottler acquireOperationThrottler = new (1, 5000);
    private RetryDeadlockPolicy retryDeadlockPolicy;

    public MasterJobsService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterJobsRepository masterJobsRepository,
        IJobMasterLogger logger,
        IJobMasterRuntime runtime,
        IKnownExceptionIdentifier exceptionIdentifier) : base(clusterConnectionConfig)
    {
        this.masterJobsRepository = masterJobsRepository;
        this.logger = logger;
        this.exceptionIdentifier = exceptionIdentifier;
        this.operationThrottler = runtime.GetOperationLimiterForCluster(clusterConnectionConfig.ClusterId);
        this.retryDeadlockPolicy = new RetryDeadlockPolicy(this.exceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);
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

    public async Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        ValidateJobExecutionOutcome(jobRaw, addJobExecution);

        try
        {
            await operationThrottler.ExecAsync(() => masterJobsRepository.UpdateAsync(jobRaw, addJobExecution));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogCategory.Job, jobRaw.Id, e);
            throw;
        }
    }

    public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        ValidateJobExecutionOutcome(jobRaw, addJobExecution);
        
        try
        {
            operationThrottler.Exec(() => masterJobsRepository.Update(jobRaw, addJobExecution));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogCategory.Job, jobRaw.Id, e);
            throw;
        }
    }

    public Task AddJobExecutionAsync(JobExecution jobExecution)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.AddJobExecutionAsync(jobExecution));
    }

    public Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.QueryJobExecutionsAsync(jobId));
    }

    public async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, DateTime expiresAtUtc)
    {
        var partitionLockId = JobMasterRandomUtil.NewGuid7();
        return await retryDeadlockPolicy.ExecAsync(() => acquireOperationThrottler.ExecAsync(() => masterJobsRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc)));
    }

    public void ReleasePartitionLock(Guid jobId)
    {
        retryDeadlockPolicy.Exec(() => acquireOperationThrottler.Exec(() => masterJobsRepository.ReleasePartitionLock(jobId)));
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

    public async Task BulkUpdateAsync(BulkJobUpdateRequest request)
    {
        if (request.JobIds.Count == 0 || request.Properties.Count == 0) return;
        await operationThrottler.ExecAsync(() => masterJobsRepository.BulkUpdateAsync(request));
    }

    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();
        return await operationThrottler.ExecAsync(() => masterJobsRepository.BulkUpdateAsync(jobs));
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
        
        if (addJobExecution != null && 
            addJobExecution.Outcome != JobExecutionOutcomeStatus.Succeeded 
            && jobRaw.Status != JobMasterJobStatus.Failed)
        {
            throw new ArgumentException("Job execution outcome must be succeeded or failed when job status is not failed.");
        }
    }
}
