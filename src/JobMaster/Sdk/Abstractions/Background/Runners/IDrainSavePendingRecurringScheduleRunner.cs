namespace JobMaster.Sdk.Abstractions.Background.Runners;

internal interface IDrainSavePendingRecurringScheduleRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}
