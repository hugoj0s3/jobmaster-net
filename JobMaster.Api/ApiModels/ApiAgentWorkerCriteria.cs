using JobMaster.Abstractions.Models;

namespace JobMaster.Api.ApiModels;

public class ApiAgentWorkerCriteria
{
    public string? AgentConnectionId { get; set; }
    public string? WorkerLane { get; set; }
    public AgentWorkerStatus? Status { get; set; }
    public AgentWorkerMode? Mode { get; set; }
    public bool? IsAlive { get; set; }
}