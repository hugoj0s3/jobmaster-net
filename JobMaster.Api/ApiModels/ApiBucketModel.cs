using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a job execution bucket as returned by the API.</summary>
public class ApiBucketModel : ApiClusterBaseModel
{
    /// <summary>Unique identifier of the bucket.</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Display name of the bucket.</summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>Identifier of the agent connection that owns this bucket.</summary>
    public string AgentConnectionId { get; set; } = null!;
    /// <summary>Name of the agent connection that owns this bucket.</summary>
    public string AgentConnectionName { get; set; } = null!;
    /// <summary>Identifier of the agent worker assigned to this bucket, if any.</summary>
    public string? AgentWorkerId { get; set; }
    /// <summary>Identifier of the host running the assigned worker, if known.</summary>
    public string? HostId { get; set; }
    /// <summary>Display name of the host running the assigned worker, if known.</summary>
    public string? HostDisplayName { get; set; }
    /// <summary>Repository type identifier for this bucket.</summary>
    public string RepositoryTypeId { get; set; } = string.Empty;
    /// <summary>Priority level of jobs held in this bucket.</summary>
    public JobMasterPriority Priority { get; set; }
    /// <summary>Current lifecycle status of the bucket.</summary>
    public BucketStatus Status { get; set; }
    /// <summary>UTC timestamp when the bucket was created.</summary>
    public DateTime CreatedAt { get;  set; }
    /// <summary>CSS hex color code assigned to this bucket for dashboard display.</summary>
    public string Color { get; set; } = string.Empty;
    /// <summary>Worker lane this bucket is dedicated to, or <c>null</c> for the default lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>UTC timestamp of the last bucket status change.</summary>
    public DateTime LastStatusChangeAt { get; set; }
    /// <summary>Classification of this bucket (Standard or Fallback).</summary>
    public BucketType BucketType { get; set; }

    internal static ApiBucketModel FromDomain(BucketModel model)
    {
        return new ApiBucketModel()
        {
            AgentConnectionId = model.AgentConnectionId.IdValue,
            AgentConnectionName = model.AgentConnectionId.Name,
            AgentWorkerId = model.AgentWorkerId,
            HostId = model.HostId?.IdValue,
            HostDisplayName = model.HostId?.HostDisplayName,
            Priority = model.Priority,
            Status = model.Status,
            WorkerLane = model.WorkerLane,
            Id = model.Id,
            Name = model.Name,
            CreatedAt = model.CreatedAt,
            Color = model.Color.ToBucketColorHex(),
            LastStatusChangeAt = model.LastStatusChangeAt,
            BucketType = model.BucketType,
            ClusterId = model.ClusterId,
            RepositoryTypeId = model.RepositoryTypeId,
        };
    }
}