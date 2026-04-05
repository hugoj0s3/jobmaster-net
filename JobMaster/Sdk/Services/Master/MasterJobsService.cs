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
    private OperationLimiter operationLimiter;

    public MasterJobsService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterJobsRepository masterJobsRepository,
        IJobMasterLogger logger,
        IJobMasterRuntime runtime) : base(clusterConnectionConfig)
    {
        this.masterJobsRepository = masterJobsRepository;
        this.logger = logger;
        this.operationLimiter = runtime.GetOperationLimiterForCluster(clusterConnectionConfig.ClusterId);
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        await operationLimiter.ExecAsync(async () =>
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
        operationLimiter.Exec(() =>
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

    public async Task UpsertAsync(JobRawModel jobRaw)
    {
        try
        {
            await operationLimiter.ExecAsync(() => DoUpsertAsync(jobRaw));
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
            operationLimiter.Exec(() => DoUpsert(jobRaw));
        }
        catch (JobMasterVersionConflictException e)
        {
            this.logger.Error("Job version conflict", JobMasterLogSubjectType.Job, jobRaw.Id, e);
            throw;
        }
    }
    
    public async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc)
    {
        return await operationLimiter.ExecAsync(() => masterJobsRepository.AcquireAndFetchAsync(queryCriteria, partitionLockId, expiresAtUtc));
    }

    public void ReleasePartitionLock(Guid jobId)
    {
        operationLimiter.Exec(() => masterJobsRepository.ReleasePartitionLock(jobId));
    }

    public IList<JobRawModel> Query(JobQueryCriteria queryCriteria)
    {
        return operationLimiter.Exec(() => masterJobsRepository.Query(queryCriteria));
    }
    
    public IList<Guid> QueryIds(JobQueryCriteria queryCriteria)
    {
        return operationLimiter.Exec(() => masterJobsRepository.QueryIds(queryCriteria));
    }

    public Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        return operationLimiter.ExecAsync(() => masterJobsRepository.QueryAsync(queryCriteria));
    }

    public Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria)
    {
        return operationLimiter.ExecAsync(() => masterJobsRepository.QueryIdsAsync(queryCriteria));
    }

    public long Count(JobQueryCriteria queryCriteria)
    {
        return operationLimiter.Exec(() => masterJobsRepository.Count(queryCriteria));
    }

    public JobRawModel? Get(Guid jobId)
    {
        return operationLimiter.Exec(() => masterJobsRepository.Get(jobId));
    }

    public Task<JobRawModel?> GetAsync(Guid jobId)
    {
        return operationLimiter.ExecAsync(() => masterJobsRepository.GetAsync(jobId));
    }

    public bool CheckVersion(Guid jobId, string? expectedVersion)
    {
        if (expectedVersion is null)
        {
            return false;
        }
        
        var job = operationLimiter.Exec(() => masterJobsRepository.Get(jobId));
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
        
        var job = await operationLimiter.ExecAsync(() => masterJobsRepository.GetAsync(jobId));
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
        
        operationLimiter.Exec(() => { masterJobsRepository.BulkUpdateStatus(jobIds, status, agentConnectionId, agentWorkerId, bucketId, negateStatuses); return true; });
    }

    private void DoUpsert(JobRawModel jobRaw) => masterJobsRepository.Upsert(jobRaw);

    private Task DoUpsertAsync(JobRawModel jobRaw) => masterJobsRepository.UpsertAsync(jobRaw);
}
