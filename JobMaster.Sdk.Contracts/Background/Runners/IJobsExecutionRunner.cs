using JobMaster.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Background.Runners;

/// <summary>
/// Interface for job execution runners that handle processing jobs within specific buckets.
/// </summary>
public interface IJobsExecutionRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId, JobMasterPriority priority);
    JobMasterPriority Priority { get; }
}