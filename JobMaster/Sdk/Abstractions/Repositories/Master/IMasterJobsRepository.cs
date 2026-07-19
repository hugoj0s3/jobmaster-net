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
    Task<JobProbeResult> ProbeForAcquireAsync(JobQueryCriteria queryCriteria);
    
    void ReleasePartitionLock(Guid jobId);
    
    Task BulkUpdateAsync(BulkJobUpdateRequest request);
    
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels);

    Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit);
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc);

    /// <summary>
    /// Selects finalized jobs eligible for purging (same candidate criteria as <see cref="PurgeFinalizedAsync"/>)
    /// as full rows, without deleting them. Used by the archive-then-delete flow.
    /// </summary>
    Task<IList<JobRawModel>> QueryFinalizedToPurgeAsync(DateTime cutoffUtc, int limit);

    /// <summary>
    /// Deletes the given jobs (plus their metadata and job executions). Shared by
    /// <see cref="PurgeFinalizedAsync"/> and the archive-then-delete flow.
    /// </summary>
    Task<int> DeleteByIdsAsync(IList<Guid> ids);

    /// <summary>
    /// Inserts each job that doesn't already exist (by cluster id + id) in this repository's cluster.
    /// Existing rows are left untouched. Every job must be in a final status.
    /// </summary>
    Task BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs);
}