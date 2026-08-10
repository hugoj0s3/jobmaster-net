using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterJobsService : IJobMasterClusterAwareService
{
    Task AddAsync(JobRawModel jobRaw);
    void Add(JobRawModel jobRaw);
    Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null);
    void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null);
    Task AddJobExecutionAsync(JobExecution jobExecution);
    Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId);

    IList<JobRawModel> Query(JobQueryCriteria queryCriteria);
    Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria);
    
    Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, DateTime expiresAtUtc);
    
    long Count(JobQueryCriteria queryCriteria);
    /// <summary>
    /// Cheap probe: returns the count of unacquired <c>OnMaster</c> jobs and the earliest
    /// <c>NextPlanExecutionAt</c>, used by <c>AssignJobsToBucketsRunner</c> to decide
    /// whether to run the imminent or scan-plan assignment path.
    /// </summary>
    Task<JobProbeResult> ProbeForAcquireAsync(JobQueryCriteria queryCriteria);
    bool CheckVersion(Guid jobId, string? version);
    Task<bool> CheckVersionAsync(Guid jobId, string? version);
    JobRawModel? Get(Guid jobId);
    Task<JobRawModel?> GetAsync(Guid jobId);
    
    /// <summary>
    /// When <paramref name="useAcquireThrottler"/> is true, routes through the same 1-at-a-time,
    /// per-cluster throttler as <see cref="AcquireAndFetchAsync"/> instead of the general
    /// operation throttler -- use this so a caller can't run concurrently with another
    /// coordinator's acquire/bulk update. Leave false for callers (e.g. execution workers
    /// persisting completion status) that shouldn't be serialized with the acquire pipeline.
    /// </summary>
    Task BulkUpdateAsync(BulkJobUpdateRequest request, bool useAcquireThrottler = false);

    /// <summary>
    /// When <paramref name="useAcquireThrottler"/> is true, routes through the same 1-at-a-time,
    /// per-cluster throttler as <see cref="AcquireAndFetchAsync"/> instead of the general
    /// operation throttler -- use this from <c>AssignJobsToBucketsRunner</c>'s OnMaster-to-InBucket
    /// bulk update so it can't run concurrently with another coordinator's acquire/bulk update.
    /// Leave false for other callers (e.g. execution workers persisting completion status) that
    /// shouldn't be serialized with the acquire pipeline.
    /// </summary>
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs, bool useAcquireThrottler = false);
}