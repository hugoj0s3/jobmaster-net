using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Contracts.Background;

public interface IBucketAwareRunner : IJobMasterClusterAwareComponent, IJobMasterRunner
{
    string? BucketId { get; set; }
}