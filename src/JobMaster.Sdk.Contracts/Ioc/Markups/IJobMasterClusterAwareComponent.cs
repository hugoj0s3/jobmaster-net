using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.Sdk.Contracts.Ioc.Markups;

public interface IJobMasterClusterAwareComponent
{ 
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
}