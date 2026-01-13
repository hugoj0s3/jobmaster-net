namespace JobMaster.Sdk.Contracts.Background.Runners;

public interface IDrainSavePendingRecurringScheduleRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}
