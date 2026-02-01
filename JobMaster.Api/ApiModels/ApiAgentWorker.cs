using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Api.ApiModels;

public class ApiAgentWorker : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string AgentConnectionId { get; set; } = null!;
    public string AgentConnectionName { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public bool IsAlive { get; set; }
    public DateTime? StopRequestedAt { get; set; }
    public TimeSpan? StopGracePeriod { get; set; }
    public DateTime LastHeartbeat { get; set; }
    public AgentWorkerMode Mode { get; set; }
    public string? WorkerLane { get; set; }
    public double ParallelismFactor { get; set; }
    public AgentWorkerStatus Status { get; set; }

    internal static ApiAgentWorker FromDomain(AgentWorkerModel model)
    {
        return new ApiAgentWorker
        {
            ClusterId = model.ClusterId,
            Id = model.Id,
            Name = model.Name,
            AgentConnectionId = model.AgentConnectionId.IdValue,
            AgentConnectionName = model.AgentConnectionId.Name,
            CreatedAt = model.CreatedAt,
            IsAlive = model.IsAlive,
            StopRequestedAt = model.StopRequestedAt,
            StopGracePeriod = model.StopGracePeriod,
            LastHeartbeat = model.LastHeartbeat,
            Mode = model.Mode,
            WorkerLane = model.WorkerLane,
            ParallelismFactor = model.ParallelismFactor,
            Status = model.Status()
        };
    }
}