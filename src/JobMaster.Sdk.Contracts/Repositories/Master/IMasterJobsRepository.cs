using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Jobs;

namespace JobMaster.Sdk.Contracts.Repositories.Master;

public interface IMasterJobsRepository : IJobMasterClusterAwareMasterRepository
{
    void Add(JobRawModel jobRaw);
    Task AddAsync(JobRawModel jobRaw);
    
    void Update(JobRawModel jobRaw);
    Task UpdateAsync(JobRawModel jobRaw);

    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    JobRawModel? Get(Guid jobId);
    
    Task<JobRawModel?> GetAsync(Guid jobId);
    
    long Count(JobQueryCriteria queryCriteria);
    
    IList<Guid> QueryIds(JobQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria);

    bool BulkUpdatePartitionLockId(IList<Guid> jobIds, int lockId, DateTime expiresAt);
    void ClearPartitionLock(Guid jobId);
    void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? excludeStatuses = null);

    Task<int> PurgeFinalByScheduledAtAsync(DateTime cutoffUtc, int limit);
}