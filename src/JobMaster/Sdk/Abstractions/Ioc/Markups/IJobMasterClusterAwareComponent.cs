using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

public interface IJobMasterClusterAwareComponent
{ 
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
}