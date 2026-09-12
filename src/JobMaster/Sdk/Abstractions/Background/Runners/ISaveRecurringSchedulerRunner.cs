namespace JobMaster.Sdk.Abstractions.Background.Runners;

internal interface ISaveRecurringSchedulerRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}