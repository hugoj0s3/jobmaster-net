namespace JobMaster.Sdk.Ioc.Setup.Json;

internal sealed class WorkerJsonConfig
{
    public string? WorkerName { get; set; }
    public string? AgentConnectionName { get; set; }
    public string? WorkerLane { get; set; }
    /// <summary>
    /// Jobs pulled from the master per transfer cycle.
    /// Maps to <see cref="JobMaster.Abstractions.JobMasterDefaults.Worker.TransferBatchSize"/> when omitted.
    /// </summary>
    public int? TransferBatchSize { get; set; }
    /// <summary>
    /// Maximum job buckets held in the worker's local in-memory buffer.
    /// Maps to <see cref="JobMaster.Abstractions.JobMasterDefaults.Worker.BucketBufferSize"/> when omitted.
    /// </summary>
    public int? BucketBufferSize { get; set; }
    public string? WorkerMode { get; set; }
    public double? ParallelismFactor { get; set; }
    public bool? SkipWarmUpTime { get; set; }
    /// <summary>Keys are JobMasterPriority names (e.g. "Medium", "High"). Values are bucket counts.</summary>
    public Dictionary<string, int>? BucketQtyConfig { get; set; }
}
