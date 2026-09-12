namespace JobMaster.Sdk.Ioc.Setup.Json;

/// <summary>
/// When <see cref="ClusterJsonConfig.Standalone"/> is <c>true</c>, only <see cref="WorkerName"/> and
/// <see cref="TransferBatchSize"/> are applied —
/// <see cref="JobMaster.Abstractions.Ioc.Selectors.IClusterStandaloneConfigSelector.AddWorker"/> has no
/// standalone equivalent of the other properties below, so they're silently ignored (same as configuring
/// a standalone cluster through the fluent API).
/// </summary>
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
