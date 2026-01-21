using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;

namespace JobMaster.Sdk.Ioc.Markups;

public abstract class JobMasterClusterAwareRepository : JobMasterClusterAwareComponent, IJobMasterClusterAwareMasterRepository
{
    protected JobMasterClusterAwareRepository(JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
    }

    public abstract string MasterRepoTypeId { get; }
}