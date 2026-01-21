namespace JobMaster.Sdk.Abstractions.Background.Runners;

public interface ISaveRecurringSchedulerRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}