using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background.Runners;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface ISaveRecurringSchedulerRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}