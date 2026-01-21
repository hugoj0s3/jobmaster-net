namespace JobMaster.Sdk.Abstractions.Background.Runners;

public interface ISavePendingJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}