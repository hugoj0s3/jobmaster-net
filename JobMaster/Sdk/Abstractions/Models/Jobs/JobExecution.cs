using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Models.Jobs;

internal class JobExecution : JobMasterBaseModel
{
    public JobExecution(string clusterId) : base(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid7();
    }

    protected JobExecution()
    {
        Id = JobMasterRandomUtil.NewGuid7();
    }
    
    public Guid Id { get; set; }
    public Guid JobId { get; set; } = Guid.Empty;
    public DateTime StartedAt { get; set; } = DateTime.UtcNow;

    public AgentConnectionId? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? BucketId { get; set; } 
    public HostId? HostId { get; set; }
    
    public DateTime? FinalizedAt { get; set; }
    public string? OutcomeMessage { get; set; }
    public JobExecutionOutcomeStatus Outcome { get; set; }
    
    public void Fail(string message)
    {
        Outcome = JobExecutionOutcomeStatus.Failed;
        FinalizedAt = DateTime.UtcNow;
        OutcomeMessage = message;
    }
    
    public void Succeed()
    {
        FinalizedAt = DateTime.UtcNow;
        Outcome = JobExecutionOutcomeStatus.Succeeded;
        OutcomeMessage = "Job execution completed successfully.";
    }
}