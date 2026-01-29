using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Ioc.Markups;

internal abstract class JobMasterClusterAwareComponent : IJobMasterClusterAwareComponent
{
    public JobMasterClusterConnectionConfig ClusterConnConfig { get; }

    public JobMasterClusterAwareComponent(JobMasterClusterConnectionConfig clusterConnConfig)
    {
        this.ClusterConnConfig = clusterConnConfig;
    }
}