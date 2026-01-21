using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IBucketAwareRunner : IJobMasterClusterAwareComponent, IJobMasterRunner
{
    string? BucketId { get; set; }
}