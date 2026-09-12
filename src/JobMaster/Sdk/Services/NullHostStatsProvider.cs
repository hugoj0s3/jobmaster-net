using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services;

internal class NullHostStatsProvider : JobMasterClusterAwareComponent, IHostStatsProvider
{
    public NullHostStatsProvider(JobMasterClusterConnectionConfig clusterConnConfig) : base(clusterConnConfig)
    {
    }

    public Task<HostStatsInfo> GetStatsAsync()
    {
        var hostStats = new HostStatsInfo()
        {
            StatisticsAt = DateTime.UtcNow,
            CpuUsagePercent = null,
            MemoryTotalBytes = null,
            MemoryUsedBytes = null,
            DiskAvailableBytes = null,
            ProcessorCount = null,
        };
        
        return Task.FromResult(hostStats);
    }
}