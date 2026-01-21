using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Background;

public interface IBucketAwareRunner : IJobMasterClusterAwareComponent, IJobMasterRunner
{
    string? BucketId { get; set; }
}