namespace JobMaster.Sdk.Contracts.Background.Runners;

public interface IDrainJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}