using JobMaster.Abstractions.Models;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Models.Buckets;

internal class BucketModel : JobMasterBaseModel
{
    public BucketModel(string clusterId) : base(clusterId)
    {
        Color = JobMasterRandomUtil.GetEnum<BucketColor>();
        CreatedAt = DateTime.UtcNow;
        LastStatusChangeAt = DateTime.UtcNow;
    }
    
    protected BucketModel() {}

    public string Id { get; internal set; } = string.Empty;
    public string Name { get; internal set; } = string.Empty;
    public AgentConnectionId AgentConnectionId { get; internal set; } = null!;
    public string? AgentWorkerId { get; internal set; }
    public string RepositoryTypeId { get; internal set; } = string.Empty;
    public JobMasterPriority Priority { get; internal set; }
    public BucketStatus Status { get; internal set; }
    public DateTime CreatedAt { get; internal set; }
    public BucketColor Color { get; internal set; }
    
    public string? WorkerLane { get; internal set; }
    
    public DateTime LastStatusChangeAt { get; internal set; }
    
    public DateTime? DeletesAt { get; internal set; }


    public override bool IsValid()
    {
        // "{ClusterId}", so Id starts with "{ClusterId}." and may have 3+ segments.
        if (string.IsNullOrWhiteSpace(Id) || !Id.StartsWith($"{ClusterId}.")) return false;

        return base.IsValid() && JobMasterStringUtils.IsValidForId(Id) && JobMasterStringUtils.IsValidForId(Name);
    }

    public void MarkAsLost()
    {
        Status = BucketStatus.Lost;
        AgentWorkerId = null;
        LastStatusChangeAt = DateTime.UtcNow;
    }
        
    public void MarkAsCompleting()
    {
        if (Status != BucketStatus.Active || string.IsNullOrEmpty(AgentWorkerId))
        {
            return;
        }

        Status = BucketStatus.Completing;
        LastStatusChangeAt = DateTime.UtcNow;
        return;
    }
    
    public bool MarkAsDraining(string agentWorkerId)
    {
        if (!CanTransitionStatusNow())
        {
            return false;
        }
        
        if (Status != BucketStatus.ReadyToDrain || string.IsNullOrEmpty(AgentWorkerId))
        {
            return false;
        }
        
        if (AgentWorkerId != agentWorkerId)
        {
            return false;
        }

        AgentWorkerId = agentWorkerId;
        Status = BucketStatus.Draining;
        LastStatusChangeAt = DateTime.UtcNow;
        return true;
    }

    public bool ReadyToDrain(string agentWorkerId)
    {
        if (!CanTransitionStatusNow())
        {
            return false;
        }
        
        if (Status != BucketStatus.Lost)
        {
            return false;
        }
        
        AgentWorkerId = agentWorkerId;
        Status = BucketStatus.ReadyToDrain;
        LastStatusChangeAt = DateTime.UtcNow;
        return true;
    }

    public bool ReadyToDelete()
    {
        if (!CanTransitionStatusNow())
        {
            return false;
        }
        
        if (Status != BucketStatus.Draining)
        {
            return false;
        }
        
        Status = BucketStatus.ReadyToDelete;
        LastStatusChangeAt = DateTime.UtcNow;
        DeletesAt = DateTime.UtcNow.Add(JobMasterConstants.BucketNoJobsBeforeReadyToDelete);
        AgentWorkerId = null;
        return true;
    }
    
    private bool CanTransitionStatusNow()
    {
        return DateTime.UtcNow.Subtract(LastStatusChangeAt) > JobMasterConstants.MinBucketStatusTransitionInterval;
    }
}