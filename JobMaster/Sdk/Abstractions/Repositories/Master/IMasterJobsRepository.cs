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

    /// <summary>Returns all JobExecutions belonging to any of the given job ids.</summary>
    Task<IList<JobExecution>> QueryJobExecutionsForJobsAsync(IList<Guid> jobIds);

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
    
    Task BulkUpdateAsync(BulkJobUpdateRequest request);
    
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels);

    /// <summary>
    /// Purges finalized jobs older than the cutoff and returns the ids that were deleted (so callers can
    /// clean up related data, e.g. JobExecution-category logs, without a second full-row query).
    /// </summary>
    Task<IList<Guid>> PurgeFinalizedAsync(DateTime cutoffUtc, int limit);
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
    /// Inserts each job that doesn't already exist (by cluster id + id) in this repository's cluster,
    /// plus every JobExecution in <paramref name="jobExecutions"/> whose JobId belongs to a job that was
    /// actually newly inserted (mirrors how JobMetadata is only copied for newly-inserted jobs — if the
    /// job already existed here, its executions are assumed to already have been archived on a prior run
    /// and are left untouched). Executions for jobs that were skipped (already existed) are silently
    /// dropped from the insert, not treated as an error. Every job must be in a final status. Returns the
    /// ids of the jobs actually newly inserted, so callers can apply the same "only newly-inserted"
    /// filtering to other data that travels alongside jobs but isn't stored by this repository (e.g. logs).
    /// </summary>
    Task<IList<Guid>> BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs, IList<JobExecution> jobExecutions);
}