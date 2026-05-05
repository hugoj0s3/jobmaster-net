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

    void ReleasePartitionLock(Guid jobId);
    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, DateTime expiresAtUtc);
    
    long Count(JobQueryCriteria queryCriteria);
    bool CheckVersion(Guid jobId, string? version);
    Task<bool> CheckVersionAsync(Guid jobId, string? version);
    JobRawModel? Get(Guid jobId);
    Task<JobRawModel?> GetAsync(Guid jobId);
    Task BulkUpdateAsync(BulkJobUpdateRequest request);

    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs);
}