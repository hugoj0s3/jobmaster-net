using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Api.ApiModels;

public class ApiBucketModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string AgentConnectionId { get; set; } = null!;
    public string AgentConnectionName { get; set; } = null!;
    public string? AgentWorkerId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }
    public string RepositoryTypeId { get; set; } = string.Empty;
    public JobMasterPriority Priority { get; set; }
    public BucketStatus Status { get; set; }
    public DateTime CreatedAt { get;  set; }
    public string Color { get; set; } = string.Empty;
    public string? WorkerLane { get; set; }
    public DateTime LastStatusChangeAt { get; set; }

    internal static ApiBucketModel FromDomain(BucketModel model)
    {
        // TODO: Mock host assignment until bucket-to-host mapping/telemetry is implemented.
        var hostSeed = HashCode.Combine(model.ClusterId, model.Id);
        var hostIndex = (Math.Abs(hostSeed) % 8) + 1;
        var hostId = $"host-{hostIndex}";
        var hostDisplayName = $"Host {hostIndex}";

        return new ApiBucketModel()
        {
            AgentConnectionId = model.AgentConnectionId.IdValue,
            AgentConnectionName = model.AgentConnectionId.Name,
            AgentWorkerId = model.AgentWorkerId,
            HostId = hostId,
            HostDisplayName = hostDisplayName,
            Priority = model.Priority,
            Status = model.Status,
            WorkerLane = model.WorkerLane,
            Id = model.Id,
            Name = model.Name,
            CreatedAt = model.CreatedAt,
            Color = model.Color.ToBucketColorHex(),
            LastStatusChangeAt = model.LastStatusChangeAt,
            ClusterId = model.ClusterId,
            RepositoryTypeId = model.RepositoryTypeId,
        };
    }
}