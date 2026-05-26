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

    /// <summary>
    /// Throws if <see cref="Succeed"/> or <see cref="Fail"/> was never called.
    /// Called by the service layer before persisting the record.
    /// </summary>
    public void EnsureFinalized()
    {
        if ((int)Outcome == 0)
            throw new InvalidOperationException($"JobExecution {Id} has no outcome set. Call Succeed() or Fail() before persisting.");
    }

    internal static JobExecution RecoverFromDb(JobExecutionPersistenceRecord rec)
    {
        var ex = new JobExecution();
        ex.ClusterId = rec.ClusterId;
        ex.Id = rec.Id;
        ex.JobId = rec.JobId;
        ex.StartedAt = rec.StartedAt;
        ex.AgentWorkerId = rec.AgentWorkerId;
        ex.BucketId = rec.BucketId;
        ex.FinalizedAt = rec.FinalizedAt;
        ex.OutcomeMessage = rec.OutcomeMessage;
        ex.Outcome = (JobExecutionOutcomeStatus)rec.Outcome;

        if (!string.IsNullOrEmpty(rec.AgentConnectionId))
        {
            ex.AgentConnectionId = new AgentConnectionId(rec.AgentConnectionId);
        }

        if (!string.IsNullOrEmpty(rec.HostId))
        {
            ex.HostId = HostId.Recover(rec.HostDisplayName ?? string.Empty, rec.HostId);
        }

        return ex;
    }
}