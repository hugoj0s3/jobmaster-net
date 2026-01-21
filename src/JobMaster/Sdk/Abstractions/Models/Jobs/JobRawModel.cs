using System.Text.Json.Serialization;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Sdk.Abstractions.Models.Jobs;

public class JobRawModel : JobMasterBaseModel
{
    public JobRawModel(string clusterId) : base(clusterId)
    {
    }
    
    [JsonConstructor]
    internal JobRawModel()
    {
    }
    
    [JsonInclude]
    public Guid Id { get; internal set; }
    [JsonInclude]
    public string JobDefinitionId { get; internal set; } = string.Empty;
    [JsonInclude]
    public JobSchedulingSourceType ScheduleSourceType { get; internal set; }
    [JsonInclude]
    public string? BucketId { get; internal set; }
    [JsonInclude]
    public AgentConnectionId? AgentConnectionId { get; internal set; }
    [JsonInclude]
    public string? AgentWorkerId { get; internal set; }
    [JsonInclude]
    public JobMasterPriority Priority { get; internal set; }
    [JsonInclude]
    public DateTime OriginalScheduledAt { get; internal set; }
    [JsonInclude]
    public DateTime ScheduledAt { get; internal set; }
    
    [JsonInclude]
    public string MsgData { get; internal set; } = "{}";
    
    [JsonInclude]
    public string? Metadata { get; internal set; }
    
    [JsonInclude]
    public JobMasterJobStatus Status { get; internal set; }
    
    [JsonInclude]
    public int NumberOfFailures { get; internal set; } 
    
    [JsonInclude]
    public TimeSpan Timeout { get; internal set; }
    
    [JsonInclude]
    public int MaxNumberOfRetries { get; internal set; }
    
    [JsonInclude]
    public DateTime CreatedAt { get; internal set; } = DateTime.UtcNow;
    
    [JsonInclude]
    public Guid? RecurringScheduleId { get; internal set; }
    
    [JsonInclude]
    public int? PartitionLockId { get; internal set; }
    
    [JsonInclude]
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    
    [JsonInclude]
    public DateTime? ProcessDeadline { get; internal set; }

    [JsonInclude]
    public DateTime? ProcessingStartedAt { get; internal set; }

    [JsonInclude]
    public DateTime? SucceedExecutedAt { get; internal set; }
    
    [JsonInclude]
    public string? WorkerLane { get; internal set; }
    
    [JsonInclude]
    public string? Version { get; internal set; }
    
    public void SetVersion(string version)
    {
        Version = version;
    }

    public Job ToJob()
    {
        return Job.FromModel(this);
    }
    
    public void AssignToBucket(AgentConnectionId agentConnectionId, string agentWorkerId, string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId) || !agentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(agentWorkerId))
        {
            throw new InvalidOperationException("invalid bucketId or agentConnectionId or agentWorkerId");
        }

        AgentConnectionId = agentConnectionId;
        AgentWorkerId = agentWorkerId;
        BucketId = bucketId;
        Status = JobMasterJobStatus.AssignedToBucket;
        RefreshDeadline();
    }

    public void AssignSavePendingJobToBucket(AgentConnectionId agentConnectionId, string agentWorkerId, string bucketId) {
        if (string.IsNullOrEmpty(bucketId) || !agentConnectionId.IsNotNullAndActive() || !agentConnectionId.IsActive())
        {   
            throw new InvalidOperationException("invalid bucketId or agentConnectionId or agentWorkerId");
        }
        
        if (Status != JobMasterJobStatus.SavePending)
        {
            throw new ArgumentException("Job is not pending.");
        }
        
        AgentConnectionId = agentConnectionId;
        AgentWorkerId = agentWorkerId;
        BucketId = bucketId;
    }
    
    public bool Enqueued()
    {
        if (!this.Status.IsBucketStatus())
        {
            return false;
        }
        
        Status = JobMasterJobStatus.Queued;
        return true;
    }
    
    public void RefreshDeadline(TimeSpan? processDeadlineDuration = null)
    {
        if (!processDeadlineDuration.HasValue)
        {
            processDeadlineDuration = JobMasterConstants.JobProcessDeadlineDuration;
        }
        
        var jobProcessDeadline = this.ScheduledAt.Add(processDeadlineDuration.Value);
        if (this.ScheduledAt < DateTime.UtcNow)
        {
            jobProcessDeadline = DateTime.UtcNow.Add(processDeadlineDuration.Value);
        }
        
        this.ProcessDeadline = jobProcessDeadline;
    }
    
    public void MarkAsHeldOnMaster()
    {
        AgentConnectionId = null;
        BucketId = null;
        AgentWorkerId = null;
        ProcessDeadline = null;
        PartitionLockId = null;
        PartitionLockExpiresAt = null;
        Status = JobMasterJobStatus.HeldOnMaster;
    } 

    public bool IsOnBoarding(TimeSpan? extraWindow = null)
    {
        var now = DateTime.UtcNow;
        var window = JobMasterConstants.ClockSkewPadding + JobMasterConstants.OnBoardingWindow;

        if (extraWindow.HasValue)
        {
            window = window + extraWindow.Value;
        }

        return ScheduledAt <= now.Add(window);
    }

    public bool TryToCancel(bool ignoreOnBoarding = false)
    {
        if (Status != JobMasterJobStatus.Processing && 
            Status != JobMasterJobStatus.Succeeded && 
            Status != JobMasterJobStatus.Failed && 
            Status != JobMasterJobStatus.Cancelled && 
            Status != JobMasterJobStatus.Queued)
        {
            
            // If it is onboarding can not be cancelled
            if (IsOnBoarding(TimeSpan.FromSeconds(5)) && !ignoreOnBoarding) 
            {
                return false;
            }
            
            Status = JobMasterJobStatus.Cancelled;
            ProcessDeadline = null;
            
            return true;
        }
        
        return false;
    }
    
    public bool TryRetry()
    {
        var maxNumberOfRetries = this.MaxNumberOfRetries;
        if (NumberOfFailures >= maxNumberOfRetries)
        {
            MarkAsFailed();
            return false;
        }
        
        var secondsToWait = 30 * Math.Pow(2, this.NumberOfFailures - 1);
        var timeToWait = TimeSpan.FromSeconds(secondsToWait);
        timeToWait += JobMasterConstants.JobProcessDeadlineDuration;
        
        ScheduledAt = DateTime.UtcNow.Add(timeToWait);
        ProcessDeadline = null;
        Status = JobMasterJobStatus.HeldOnMaster;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        NumberOfFailures++;
        return true;
    }
    
    public void MarkAsFailed()
    {
        Status = JobMasterJobStatus.Failed;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        ProcessDeadline = null;
        NumberOfFailures++;
    }
    
    public void MarkAsSucceeded()
    {
        Status = JobMasterJobStatus.Succeeded;
        SucceedExecutedAt = DateTime.UtcNow;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        ProcessDeadline = null;
    }
    
    public void ReSchedule(DateTime scheduledAt)
    {
        if (this.Status != JobMasterJobStatus.Succeeded && this.Status != JobMasterJobStatus.Failed && this.Status != JobMasterJobStatus.Cancelled)
        {
            throw new ArgumentException("Job must be succeeded, failed or cancelled.");
        }
        
        if (this.Status == JobMasterJobStatus.Processing)
        {
            throw new ArgumentException("Job is already running.");
        }
        
        this.Status = JobMasterJobStatus.HeldOnMaster;
        this.ScheduledAt = scheduledAt;
        this.AgentConnectionId = null;
        this.BucketId = null;
        this.AgentWorkerId = null;
        this.ProcessDeadline = null;
    }
    
    public bool CanReSchedule()
    {
        if (this.Status.IsFinalStatus())
        {
            return false;
        }
        
        if (this.Status == JobMasterJobStatus.Processing)
        {
            return false;
        }

        if (this.IsOnBoarding(TimeSpan.FromSeconds(5)))
        {
            return false;
        }
        
        return true;
    }

    public void ProcessingStarted()
    {
        Status = JobMasterJobStatus.Processing;
        ProcessingStartedAt = DateTime.UtcNow;
        RefreshDeadline(Timeout + JobMasterConstants.JobProcessDeadlineDuration);
    }
    
    public bool ExceedProcessDeadline()
    {
        return ProcessDeadline.HasValue && ProcessDeadline.Value < DateTime.UtcNow.Add(JobMasterConstants.ClockSkewPadding).Add(TimeSpan.FromMinutes(1));
    }
    
    public bool CanHeldOnMasterExceedDeadline()
    {
        if (this.Status == JobMasterJobStatus.HeldOnMaster)
        {
            return false;
        }

        if (!this.ProcessDeadline.HasValue)
        {
            return false;
        }

        var nowWithSkew = DateTime.UtcNow.Add(JobMasterConstants.ClockSkewPadding);
        var threshold = nowWithSkew.Add(TimeSpan.FromMinutes(1));
        return this.ProcessDeadline.Value <= threshold;
    }
    
    public int CalcEstimateByteSize()
    {
        return JobMasterRawMessage.CalcEstimateByteSize(this);
    }
    
    public static JobRawModel RecoverFromDb(JobPersistenceRecord d)
        => JobConvertUtil.FromPersistence(d);

    public static JobPersistenceRecord ToPersistence(JobRawModel m)
        => JobConvertUtil.ToPersistence(m);
}