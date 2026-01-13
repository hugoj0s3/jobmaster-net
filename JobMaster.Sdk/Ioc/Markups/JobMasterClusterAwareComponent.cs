using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Ioc.Markups;

public abstract class JobMasterClusterAwareComponent : IJobMasterClusterAwareComponent
{
    public JobMasterClusterConnectionConfig ClusterConnConfig { get; }

    public JobMasterClusterAwareComponent(JobMasterClusterConnectionConfig clusterConnConfig)
    {
        this.ClusterConnConfig = clusterConnConfig;
    }
}