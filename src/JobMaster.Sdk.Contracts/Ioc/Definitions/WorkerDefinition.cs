using JobMaster.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Ioc.Definitions;

public sealed class WorkerDefinition
{
    public string ClusterId { get; set; } = string.Empty;
    public string AgentConnectionName { get; set; } = string.Empty;
    public string WorkerName { get; set; } = string.Empty;
    public string? WorkerLane { get; set; } = null;
    public int BatchSize { get; set; } = 250;
    public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Standalone;
    public double ParallelismFactor { get; set; } = 1;
    public IDictionary<JobMasterPriority, int> BucketQty { get; } = new Dictionary<JobMasterPriority, int>()
    {
        { JobMasterPriority.VeryLow, 1 },
        { JobMasterPriority.Low, 1 },
        { JobMasterPriority.Medium, 1 },
        { JobMasterPriority.High, 1 },
        { JobMasterPriority.Critical, 1 }
    };

    public bool SkipWarmUpTime { get; set; }
}