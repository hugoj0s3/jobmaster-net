namespace JobMaster.Sdk.Abstractions.Background.Runners;

internal interface ISavePendingJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}