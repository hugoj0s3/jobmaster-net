using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a host (process/machine) running JobMaster workers as returned by the API.</summary>
public class ApiHostModel
{
    /// <summary>Unique identifier of the host.</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Human-readable display name of the host.</summary>
    public string HostDisplayName { get; set; } = string.Empty;
    /// <summary>Operating system process identifier of the host process.</summary>
    public string ProcessId { get; set; } = null!;
    /// <summary>Number of logical processors available on the host.</summary>
    public int? ProcessorCount { get; set; }
    /// <summary>Description of the host operating system.</summary>
    public string? OsDescription { get; set; }
    /// <summary>UTC timestamp of the last heartbeat received from this host.</summary>
    public DateTime? LastHeartbeat { get; set; }
    /// <summary>UTC timestamp when the host was first registered.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>UTC timestamp of the last statistics collection on this host.</summary>
    public DateTime? StatisticsAt { get; set; }
    /// <summary>CPU usage percentage at last statistics collection.</summary>
    public double? CpuUsagePercent { get; set; }
    /// <summary>Total physical memory in bytes.</summary>
    public long? MemoryTotalBytes { get; set; }
    /// <summary>Used physical memory in bytes at last statistics collection.</summary>
    public long? MemoryUsedBytes { get; set; }
    /// <summary>Available disk space in bytes at last statistics collection.</summary>
    public long? DiskAvailableBytes { get; set; }
    /// <summary>Whether the host is currently alive (sending heartbeats).</summary>
    public bool IsAlive { get; set; }

    internal static ApiHostModel FromDomain(HostModel host)
    {
        return new ApiHostModel
        {
            Id = host.Id.IdValue,
            HostDisplayName = host.Id.HostDisplayName,
            ProcessId = host.ProcessId,
            OsDescription = host.OsDescription,
            LastHeartbeat = host.LastHeartbeat,
            CreatedAt = host.CreatedAt,
            StatisticsAt = host.StatisticsAt == default ? host.LastStats?.StatisticsAt : host.StatisticsAt,
            CpuUsagePercent = host.CpuUsagePercent ?? host.LastStats?.CpuUsagePercent,
            MemoryTotalBytes = host.MemoryTotalBytes ?? host.LastStats?.MemoryTotalBytes,
            MemoryUsedBytes = host.MemoryUsedBytes ?? host.LastStats?.MemoryUsedBytes,
            DiskAvailableBytes = host.DiskAvailableBytes ?? host.LastStats?.DiskAvailableBytes,
            ProcessorCount = host.ProcessorCount,
            IsAlive = host.IsAlive(),
        };
    }
}