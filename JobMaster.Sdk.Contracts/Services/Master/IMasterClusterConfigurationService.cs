using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Services.Master;

public interface IMasterClusterConfigurationService : IJobMasterClusterAwareService
{
    ClusterConfigurationModel? Get();
    
    ClusterConfigurationModel? GetNoAche();

    void Save(ClusterConfigurationModel clusterConfiguration);
    
    bool IsSaved();
}