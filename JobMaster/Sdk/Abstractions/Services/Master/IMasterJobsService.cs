using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterJobsService : IJobMasterClusterAwareService
{
    Task AddAsync(JobRawModel jobRaw);
    void Add(JobRawModel jobRaw);

    /// <summary>
    /// When <paramref name="throttler"/> is omitted, routes through the general per-cluster
    /// operation throttler. Pass an explicit throttler (e.g. <see cref="AcquireThrottler"/>, or a
    /// caller-owned instance) to isolate this update from unrelated operations competing for the
    /// same limiter -- e.g. an execution worker persisting completion status shouldn't have to
    /// queue behind whatever else is using the shared general throttler at that moment.
    /// </summary>
    Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null, OperationThrottler? throttler = null);

    /// <inheritdoc cref="UpdateAsync"/>
    void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null, OperationThrottler? throttler = null);
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
    /// The same 1-at-a-time, per-cluster throttler used internally by
    /// <see cref="AcquireAndFetchAsync"/> -- exposed so a caller (e.g.
    /// <c>AssignJobsToBucketsRunner</c>'s OnMaster-to-InBucket bulk update, or
    /// <c>WorkerClusterOperations</c>'s dispatch-failure rollback) can explicitly pass it into
    /// <see cref="BulkUpdateAsync(BulkJobUpdateRequest,OperationThrottler)"/> /
    /// <see cref="BulkUpdateAsync(IList{JobRawModel},OperationThrottler)"/> when that call must not
    /// run concurrently with another coordinator's acquire/bulk update.
    /// </summary>
    OperationThrottler AcquireThrottler { get; }

    /// <summary>
    /// When <paramref name="throttler"/> is omitted, routes through the general per-cluster
    /// operation throttler. Pass <see cref="AcquireThrottler"/> so this call can't run
    /// concurrently with another coordinator's acquire/bulk update, or any other explicit
    /// throttler instance the caller wants this isolated to.
    /// </summary>
    Task BulkUpdateAsync(BulkJobUpdateRequest request, OperationThrottler? throttler = null);

    /// <inheritdoc cref="BulkUpdateAsync(BulkJobUpdateRequest,OperationThrottler)"/>
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs, OperationThrottler? throttler = null);
}