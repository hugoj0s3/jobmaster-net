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
    public int ProcessorCount { get; set; }
    public string? OsDescription { get; set; }
    
    public DateTime LastHeartbeat { get; set; }
    public DateTime CreatedAt { get; set; }
    public HostStatsModel? LastStats { get; set; } = null!;
}