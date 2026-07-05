using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal sealed class WorkerDefinition
{
    public string ClusterId { get; set; } = string.Empty;
    public string AgentConnectionName { get; set; } = string.Empty;
    public string? WorkerName { get; set; } = string.Empty;
    public string? WorkerLane { get; set; } = null;
    public int TransferBatchSize { get; set; } = JobMasterDefaults.Worker.TransferBatchSize;
    public int BucketBufferSize { get; set; } = JobMasterDefaults.Worker.BucketBufferSize;
    public AgentWorkerMode Mode { get; set; } = JobMasterDefaults.Worker.DefaultMode;
    public double ParallelismFactor { get; set; } = JobMasterDefaults.Worker.ParallelismFactor;
    /// <summary>
    /// Explicit per-priority bucket counts. Absent entries default to
    /// <see cref="JobMasterDefaults.Worker.BucketQtyPerPriority"/> at startup.
    /// Set a priority to 0 to suppress bucket creation for it.
    /// </summary>
    public IDictionary<JobMasterPriority, int> BucketQty { get; } = new Dictionary<JobMasterPriority, int>();

    public bool SkipWarmUpTime { get; set; }
    public TimeSpan BucketBufferLeadTime { get; set; } = JobMasterDefaults.Worker.BucketBufferLeadTime;
}
