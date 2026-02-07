namespace JobMaster.Api.ApiModels;

public class ApiHostModel
{
    public string Id { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    
    public double? CpuUsagePercent { get; set; }
    public long? MemoryTotalBytes { get; set; }
    public long? MemoryUsedBytes { get; set; }
    
    public int ThreadCount { get; set; }
    public int HandleCount { get; set; }
}