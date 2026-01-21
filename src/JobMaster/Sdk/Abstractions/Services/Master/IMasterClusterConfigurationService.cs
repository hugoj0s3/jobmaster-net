using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Services.Master;

public interface IMasterClusterConfigurationService : IJobMasterClusterAwareService
{
    ClusterConfigurationModel? Get();
    
    ClusterConfigurationModel? GetNoAche();

    void Save(ClusterConfigurationModel clusterConfiguration);
    
    bool IsSaved();
}