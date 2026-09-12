using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterHostService : IJobMasterClusterAwareService
{
    Task<HostId> RegisterNewHostAsync();
    
    Task UpdateStatsAsync(string hostId);
    
    Task DeleteHostsAsync(IList<string> hostIds);
    Task<IList<HostModel>> QueryAllAsync();
}