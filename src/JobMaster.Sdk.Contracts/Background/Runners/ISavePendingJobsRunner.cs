namespace JobMaster.Sdk.Contracts.Background.Runners;

public interface ISavePendingJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}