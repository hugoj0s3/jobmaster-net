using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Sdk.Abstractions.Services;

internal interface IHostStatsProvider : IJobMasterClusterAwareService
{
    Task<HostStatsInfo> GetStatsAsync();
}