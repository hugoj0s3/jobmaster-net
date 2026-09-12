using JobMaster.Sdk.Abstractions;

namespace JobMaster.Sdk.Abstractions.Models.Hosts;

internal class HostModel : JobMasterBaseModel
{
    public HostModel(string clusterId) : base(clusterId)
    {
    }

    protected HostModel()
    {
    }

    public HostId Id { get; set; } = null!;

    public string ProcessId { get; set; } = null!;
    public int? ProcessorCount { get; set; }
    public string? OsDescription { get; set; }

    public DateTime LastHeartbeat { get; set; }
    public DateTime CreatedAt { get; set; }
    public HostStatsInfo? LastStats { get; set; } = null!;
    public double? CpuUsagePercent { get; set; }
    public DateTime StatisticsAt { get; set; }
    public long? DiskAvailableBytes { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }

    public bool IsAlive() => LastHeartbeat > DateTime.UtcNow - JobMasterConstants.ResourceAliveThreshold;
}