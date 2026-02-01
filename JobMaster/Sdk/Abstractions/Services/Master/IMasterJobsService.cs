using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterJobsService : IJobMasterClusterAwareService
{
    Task AddAsync(JobRawModel jobRaw);
    void Add(JobRawModel jobRaw);
    Task UpsertAsync(JobRawModel jobRaw);
    void Upsert(JobRawModel jobRaw);

    [Obsolete("Use AcquireAndFetchAsync(...) instead. This method will be removed in a future release.")]
    bool BulkUpdatePartitionLockId(IList<Guid> jobIds, int lockId, DateTime expiresAt);

    void ReleasePartitionLock(Guid jobId);

    [Obsolete("Use ReleasePartitionLock(...) instead. This method will be removed in a future release.")]
    void ClearPartitionLock(Guid jobId);
    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    IList<Guid> QueryIds(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc);
    
    Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria);
    long Count(JobQueryCriteria queryCriteria);
    bool CheckVersion(Guid jobId, string? version);
    Task<bool> CheckVersionAsync(Guid jobId, string? version);
    JobRawModel? Get(Guid jobId);
    Task<JobRawModel?> GetAsync(Guid jobId);
    void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? excludeStatuses = null);
}