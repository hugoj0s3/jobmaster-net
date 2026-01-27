using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Ioc.Markups;

internal interface IJobMasterClusterAwareComponent
{ 
    JobMasterClusterConnectionConfig ClusterConnConfig { get; }
}