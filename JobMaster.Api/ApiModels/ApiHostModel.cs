using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Api.ApiModels;

public class ApiHostModel
{
    public string Id { get; set; } = string.Empty;
    public string HostDisplayName { get; set; } = string.Empty;
    
    public string ProcessId { get; set; } = null!;
    public int ProcessorCount { get; set; }
    public string? OsDescription { get; set; }
    
    public DateTime? LastHeartbeat { get; set; }
    public DateTime CreatedAt { get; set; }
    
    public DateTime? StatisticsAt { get; set; }
    
    public double? CpuUsagePercent { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }
    
    public int? ThreadCount { get; set; }
    public int? HandleCount { get; set; }
   
    public long? GcTotalMemory { get; set; }
    
    public long? DiskAvailableBytes { get; set; }
    
    public bool IsAlive { get; set; }

    internal static ApiHostModel FromDomain(HostModel host)
    {
        return new ApiHostModel
        {
            Id = host.Id.IdValue,
            HostDisplayName = host.Id.HostDisplayName,
            ProcessId = host.ProcessId,
            ProcessorCount = host.ProcessorCount,
            OsDescription = host.OsDescription,
            LastHeartbeat = host.LastHeartbeat,
            CreatedAt = host.CreatedAt,
            StatisticsAt = host.LastStats?.StatisticsAt,
            CpuUsagePercent = host.LastStats?.CpuUsagePercent,
            MemoryTotalBytes = host.LastStats?.MemoryTotalBytes,
            MemoryUsedBytes = host.LastStats?.MemoryUsedBytes,
            ThreadCount = host.LastStats?.ThreadCount,
            HandleCount = host.LastStats?.HandleCount,
            GcTotalMemory = host.LastStats?.GcTotalMemory,
            DiskAvailableBytes = host.LastStats?.DiskAvailableBytes,
            IsAlive = host.IsAlive(),
        };
    }
}