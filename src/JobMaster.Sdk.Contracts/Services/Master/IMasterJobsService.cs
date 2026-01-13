using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Jobs;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterJobsService : IJobMasterClusterAwareService
{
    Task AddAsync(JobRawModel jobRaw);
    void Add(JobRawModel jobRaw);
    Task UpsertAsync(JobRawModel jobRaw);
    void Upsert(JobRawModel jobRaw);
    bool BulkUpdatePartitionLockId(IList<Guid> jobIds, int lockId, DateTime expiresAt);
    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    IList<Guid> QueryIds(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria);
    long Count(JobQueryCriteria queryCriteria);
    bool CheckVersion(Guid jobId, string? version);
    Task<bool> CheckVersionAsync(Guid jobId, string? version);
    JobRawModel? Get(Guid jobId);
    Task<JobRawModel?> GetAsync(Guid jobId);
    void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? excludeStatuses = null);
}