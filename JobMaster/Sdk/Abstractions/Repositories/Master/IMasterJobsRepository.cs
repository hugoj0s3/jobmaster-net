using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

internal interface IMasterJobsRepository : IJobMasterClusterAwareMasterRepository
{
    void Add(JobRawModel jobRaw);
    Task AddAsync(JobRawModel jobRaw);

    void Upsert(JobRawModel jobRaw);
    Task UpsertAsync(JobRawModel jobRaw);

    bool Exists(Guid jobId);
    Task<bool> ExistsAsync(Guid jobId);

    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    JobRawModel? Get(Guid jobId);
    
    Task<JobRawModel?> GetAsync(Guid jobId);
    
    long Count(JobQueryCriteria queryCriteria);
    
    IList<Guid> QueryIds(JobQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria);
    
    void ReleasePartitionLock(Guid jobId);
    
    void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? excludeStatuses = null);

    Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit);
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc);
}