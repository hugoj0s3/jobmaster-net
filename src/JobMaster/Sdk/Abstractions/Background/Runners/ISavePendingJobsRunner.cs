using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background.Runners;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface ISavePendingJobsRunner : IBucketAwareRunner
{
    void DefineBucketId(string bucketId);
}