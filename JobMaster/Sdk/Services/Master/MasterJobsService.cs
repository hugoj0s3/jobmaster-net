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

namespace JobMaster.Sdk.Services.Master;

internal class MasterJobsService : JobMasterClusterAwareComponent, IMasterJobsService
{
    private IMasterJobsRepository masterJobsRepository = null!;
    private IJobMasterLogger logger = null!;
    private OperationThrottler operationThrottler;

    public MasterJobsService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterJobsRepository masterJobsRepository,
        IJobMasterLogger logger,
        IJobMasterRuntime runtime) : base(clusterConnectionConfig)
    {
        this.masterJobsRepository = masterJobsRepository;
        this.logger = logger;
        this.operationThrottler = runtime.GetOperationThrottlerForCluster(clusterConnectionConfig.ClusterId);
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        await operationThrottler.ExecAsync(async () =>
        {
            try
            {
                await masterJobsRepository.AddAsync(jobRaw);
            }
            catch (JobDuplicationException) 
            {
                throw;
            }
            catch (Exception ex)
            {
                var exists = await this.masterJobsRepository.GetAsync(jobRaw.Id);
                if (exists is not null)
                {
                    throw new JobDuplicationException(jobRaw.Id, ex);
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
                if (this.masterJobsRepository.Get(jobRaw.Id) is not null)
                {
                    throw new JobDuplicationException(jobRaw.Id, ex);
                }
                
                throw;
            }
        });
    }

    public async Task UpsertAsync(JobRawModel jobRaw)
    {
        try
        {
            await operationThrottler.ExecAsync(() => DoUpsertAsync(jobRaw));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogSubjectType.Job, jobRaw.Id, e);
            throw;
        }
    }

    public void Upsert(JobRawModel jobRaw)
    {
        try
        {
            operationThrottler.Exec(() => DoUpsert(jobRaw));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogSubjectType.Job, jobRaw.Id, e);
            throw;
        }
    }

    [Obsolete("Use AcquireAndFetchAsync(...) instead. This method will be removed in a future release.")]
    public bool BulkUpdatePartitionLockId(IList<Guid> jobIds, int lockId, DateTime expiresAt)
    {   if (jobIds.Count <= 0)
        {
            return false;
        }
        
        return operationThrottler.Exec(() => masterJobsRepository.BulkUpdatePartitionLockId(jobIds, lockId, expiresAt));
    }
    
    public async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc)
    {
        return await operationThrottler.ExecAsync(() => masterJobsRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc));
    }

    public void ReleasePartitionLock(Guid jobId)
    {
        operationThrottler.Exec(() => masterJobsRepository.ReleasePartitionLock(jobId));
    }

    [Obsolete("Use ReleasePartitionLock(...) instead. This method will be removed in a future release.")]
    public void ClearPartitionLock(Guid jobId)
    {
        ReleasePartitionLock(jobId);
    }

    public IList<JobRawModel> Query(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterJobsRepository.Query(queryCriteria));
    }
    
    public IList<Guid> QueryIds(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.Exec(() => masterJobsRepository.QueryIds(queryCriteria));
    }

    public Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.QueryAsync(queryCriteria));
    }

    public Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria)
    {
        return operationThrottler.ExecAsync(() => masterJobsRepository.QueryIdsAsync(queryCriteria));
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

    public void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? negateStatuses = null)
    {
        if (jobIds.Count <= 0)
        {
            return;
        }
        
        operationThrottler.Exec(() => { masterJobsRepository.BulkUpdateStatus(jobIds, status, agentConnectionId, agentWorkerId, bucketId, negateStatuses); return true; });
    }

    private void DoUpsert(JobRawModel jobRaw)
    {
        var jobEntity = masterJobsRepository.Get(jobRaw.Id);
        if (jobEntity is null)
        {
            masterJobsRepository.Add(jobRaw);
            return;
        }

        masterJobsRepository.Update(jobRaw);
    }
    
    private async Task DoUpsertAsync(JobRawModel jobRaw)
    {
        var jobEntity = await masterJobsRepository.GetAsync(jobRaw.Id);
        if (jobEntity == null)
        {
            await masterJobsRepository.AddAsync(jobRaw);
            return;
        }

        await masterJobsRepository.UpdateAsync(jobRaw);
    }
}