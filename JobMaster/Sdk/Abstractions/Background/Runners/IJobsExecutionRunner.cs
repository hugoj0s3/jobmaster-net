using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Background.Runners;

/// <summary>
/// Interface for job execution runners that handle processing jobs within specific buckets.
/// </summary>
internal interface IJobsExecutionRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId, JobMasterPriority priority);
    JobMasterPriority Priority { get; }
}