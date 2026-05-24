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

    void ReleasePartitionLock(Guid jobId);
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
    Task BulkUpdateAsync(BulkJobUpdateRequest request);

    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs);
}