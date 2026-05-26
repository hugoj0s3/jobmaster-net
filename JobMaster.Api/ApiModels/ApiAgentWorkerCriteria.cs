using JobMaster.Abstractions.Models;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for listing agent workers.</summary>
public class ApiAgentWorkerCriteria
{
    /// <summary>Filter by agent connection ID.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>Filter by worker lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>Filter by worker status.</summary>
    public AgentWorkerStatus? Status { get; set; }
    /// <summary>Filter by worker mode.</summary>
    public AgentWorkerMode? Mode { get; set; }
    /// <summary>Filter by alive status.</summary>
    public bool? IsAlive { get; set; }
    /// <summary>Maximum number of results to return.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
    public int? Offset { get; set; }
    /// <summary>Optional sort specification.</summary>
    public ApiSortByCriteria? SortBy { get; set; }
}