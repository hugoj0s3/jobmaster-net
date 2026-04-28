using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterClusterConfigurationService : IJobMasterClusterAwareService
{
    ClusterConfigurationModel? Get();
    
    ClusterConfigurationModel? GetFresh();

    void Save(ClusterConfigurationModel clusterConfiguration);
    
    bool IsSaved();
}