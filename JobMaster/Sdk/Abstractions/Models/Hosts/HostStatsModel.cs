namespace JobMaster.Sdk.Abstractions.Models.Hosts;

internal class HostStatsModel : JobMasterBaseModel
{
    public HostStatsModel(string clusterId) : base(clusterId)
    {
    }
    
    protected HostStatsModel()
    {
    }
    
    public Guid Id { get; set; } = Guid.NewGuid();
    public HostId HostId { get; set; } = null!;
    public DateTime StatisticsAt { get; set; }
    
    public double? CpuUsagePercent { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }
    
    public int ThreadCount { get; set; }
    public int HandleCount { get; set; }
   
    public long GcTotalMemory { get; set; }
    
    public long? DiskAvailableBytes { get; set; }
}