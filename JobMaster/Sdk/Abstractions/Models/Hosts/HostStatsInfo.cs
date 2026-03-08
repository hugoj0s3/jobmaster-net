namespace JobMaster.Sdk.Abstractions.Models.Hosts;

internal class HostStatsInfo
{
    public HostStatsInfo() { }
    
    public DateTime StatisticsAt { get; set; }
    
    public double? CpuUsagePercent { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }
    public long? DiskAvailableBytes { get; set; }
    public int? ProcessorCount { get; set; }
}