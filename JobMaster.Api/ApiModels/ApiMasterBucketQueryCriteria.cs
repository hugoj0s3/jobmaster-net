using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Utils;

namespace JobMaster.Api.ApiModels;

public class ApiMasterBucketQueryCriteria
{
    public string? AgentConnectionId { get; set; }
    public JobMasterPriority? Priority { get; set; }
    public BucketStatus? Status { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? WorkerLane { get; set; }

    internal MasterBucketQueryCriteria ToDomainCriteria()
    {
        return new MasterBucketQueryCriteria()
        {
            AgentConnectionId = AgentConnectionId,
            AgentWorkerId = AgentWorkerId,
            Priority = Priority,
            Status = Status,
            WorkerLane = WorkerLane,
        };
    }
}