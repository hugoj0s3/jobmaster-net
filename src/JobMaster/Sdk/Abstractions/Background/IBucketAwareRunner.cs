using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IBucketAwareRunner : IJobMasterClusterAwareComponent, IJobMasterRunner
{
    string? BucketId { get; set; }
}