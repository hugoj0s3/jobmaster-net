namespace JobMaster.Sdk.Contracts.Background.Runners;

public interface ISaveRecurringSchedulerRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}