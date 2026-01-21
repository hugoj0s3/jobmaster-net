namespace JobMaster.Sdk.Abstractions.Background.Runners;

public interface IDrainSavePendingRecurringScheduleRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}
