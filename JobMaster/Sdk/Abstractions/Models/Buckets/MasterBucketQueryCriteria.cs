using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Models.Buckets;

internal class MasterBucketQueryCriteria
{
    public string? AgentConnectionId { get; set; }
    public JobMasterPriority? Priority { get; set; }
    public BucketStatus? Status { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? WorkerLane { get; set; }
    public ReadIsolationLevel ReadIsolationLevel { get; set; } = ReadIsolationLevel.Consistent;
}