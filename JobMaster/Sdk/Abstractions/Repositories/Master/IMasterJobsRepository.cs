using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Repositories.Master;

internal interface IMasterJobsRepository : IJobMasterClusterAwareMasterRepository
{
    void Add(JobRawModel jobRaw);
    Task AddAsync(JobRawModel jobRaw);
    void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null);
    Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null);
    Task AddJobExecutionAsync(JobExecution jobExecution);
    Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId);

    bool Exists(Guid jobId);
    Task<bool> ExistsAsync(Guid jobId);

    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    JobRawModel? Get(Guid jobId);
    
    Task<JobRawModel?> GetAsync(Guid jobId);
    
    long Count(JobQueryCriteria queryCriteria);
    /// <summary>
    /// Returns the count of unacquired <c>OnMaster</c> jobs matching the criteria and the earliest
    /// <c>NextPlanExecutionAt</c> among them. MetadataFilters are not supported and will throw.
    /// </summary>
    Task<JobProbeResult> ProbeForBucketAssignmentAsync(JobQueryCriteria queryCriteria);
    
    void ReleasePartitionLock(Guid jobId);
    
    Task BulkUpdateAsync(BulkJobUpdateRequest request);
    
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels);

    Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit);
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc);
}