namespace JobMaster.Sdk.Abstractions.Models.Hosts;

internal class HostStatsInput
{
    public double? CpuUsagePercent { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }
    
    public int ThreadCount { get; set; }
    public int HandleCount { get; set; }
   
    public long GcTotalMemory { get; set; }
    
    public long? DiskAvailableBytes { get; set; }
}