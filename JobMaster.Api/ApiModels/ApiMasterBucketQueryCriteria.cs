using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Utils;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for filtering and paginating buckets from the master store.</summary>
public class ApiMasterBucketQueryCriteria
{
    /// <summary>Filter by agent connection ID.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>Filter by bucket priority.</summary>
    public JobMasterPriority? Priority { get; set; }
    /// <summary>Filter by bucket lifecycle status.</summary>
    public BucketStatus? Status { get; set; }
    /// <summary>Filter by assigned worker ID.</summary>
    public string? AgentWorkerId { get; set; }
    /// <summary>Filter by worker lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>Filter by bucket type.</summary>
    public BucketType? BucketType { get; set; }
    /// <summary>Maximum number of results to return.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
    public int? Offset { get; set; }
    /// <summary>Filter by specific bucket IDs.</summary>
    public string[]? BucketIds { get; set; }
    /// <summary>Optional sort specification.</summary>
    public ApiSortByCriteria? SortBy { get; set; }

    internal MasterBucketQueryCriteria ToDomainCriteria()
    {
        return new MasterBucketQueryCriteria()
        {
            AgentConnectionId = AgentConnectionId,
            AgentWorkerId = AgentWorkerId,
            Priority = Priority,
            Status = Status,
            WorkerLane = WorkerLane,
            BucketType = BucketType,
            BucketIds = BucketIds ?? [],
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
        };
    }
}