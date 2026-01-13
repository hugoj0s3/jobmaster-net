using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc.Markups;

namespace JobMaster.Sdk.Ioc.Markups;

public abstract class JobMasterClusterAwareRepository : JobMasterClusterAwareComponent, IJobMasterClusterAwareMasterRepository
{
    protected JobMasterClusterAwareRepository(JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
    }

    public abstract string MasterRepoTypeId { get; }
}