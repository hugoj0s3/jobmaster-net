using JobMaster.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Models.Buckets;

public class MasterBucketQueryCriteria
{
    public string? AgentConnectionId { get; set; }
    public JobMasterPriority? Priority { get; set; }
    public BucketStatus? Status { get; set; }
    public string? AgentWorkerId { get; set; }
    public bool? OnlyActive { get; set; }
}