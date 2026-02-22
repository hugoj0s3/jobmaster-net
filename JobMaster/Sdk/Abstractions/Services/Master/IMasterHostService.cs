using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterHostService : IJobMasterClusterAwareService
{
    Task<IList<HostModel>> QueryAllAsync();
    
    Task<HostId> RegisterNewHostAsync();
    
    Task<IList<HostStatsModel>> QueryAllStatsAsync(string hostId);
    
    Task AddStatsAsync(string hostId);
    
    Task DeleteHostsAsync(IList<string> hostIds);
}