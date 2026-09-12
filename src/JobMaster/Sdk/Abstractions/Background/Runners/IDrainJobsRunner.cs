namespace JobMaster.Sdk.Abstractions.Background.Runners;

internal interface IDrainJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}