using System.Text.Json.Serialization;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Sdk.Abstractions.Models.Jobs;

internal class JobRawModel : JobMasterBaseModel
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
    public JobMasterTriggerSourceType TriggerSourceType { get; internal set; }
    
    [JsonInclude]
    public Guid? SourceId { get; internal set; }
    
    [JsonInclude]
    public string? BucketId { get; internal set; }
    [JsonInclude]
    public AgentConnectionId? AgentConnectionId { get; internal set; }
    [JsonInclude]
    public string? AgentWorkerId { get; internal set; }
    [JsonInclude]
    public JobMasterPriority Priority { get; internal set; }
    [JsonInclude]
    public DateTime ScheduledAt { get; internal set; }
    [JsonInclude]
    public DateTime? NextPlanExecutionAt { get; internal set; }
    
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
    public Guid? PartitionLockId { get; internal set; }
    
    [JsonInclude]
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    
    [JsonInclude]
    public DateTime? ProcessDeadline { get; internal set; }

    [JsonInclude]
    public DateTime? ProcessStartedAt { get; internal set; }

    [JsonInclude]
    public DateTime? FinalizedAt { get; internal set; }
    
    [JsonInclude]
    public string? WorkerLane { get; internal set; }
    
    [JsonInclude]
    public string? Version { get; internal set; }
    
    [JsonInclude]
    public HostId? HostId { get; internal set; }
    
    public void SetVersion(string version)
    {
        Version = version;
    }

    public DateTime GetSafeNextPlanExecutionAt()
    {
        return this.NextPlanExecutionAt ?? this.ScheduledAt;
    }

    public Job ToJob()
    {
        return Job.FromModel(this);
    }
    
    public void AssignToBucket(BucketModel bucketModel)
    {
        if (!bucketModel.CanAssign())
        {
            throw new InvalidOperationException("invalid bucketId or agentConnectionId or agentWorkerId");
        }
        
        var agentConnectionId = bucketModel.AgentConnectionId;
        var agentWorkerId = bucketModel.AgentWorkerId;
        var bucketId = bucketModel.Id;
        var hostId = bucketModel.HostId;

        AgentConnectionId = agentConnectionId;
        AgentWorkerId = agentWorkerId;
        BucketId = bucketId;
        HostId = hostId;
        Status = JobMasterJobStatus.InBucket;
        RefreshDeadline();
    }

    public void AssignSavePendingJobToBucket(BucketModel bucketModel) {
        
        if (!bucketModel.CanAssign())
        {
            throw new InvalidOperationException("invalid bucketId or agentConnectionId or agentWorkerId");
        }
        
        var agentConnectionId = bucketModel.AgentConnectionId;
        var agentWorkerId = bucketModel.AgentWorkerId;
        var bucketId = bucketModel.Id;
        var hostId = bucketModel.HostId;
        
        AgentConnectionId = agentConnectionId;
        AgentWorkerId = agentWorkerId;
        BucketId = bucketId;
        HostId = hostId;
        Status = JobMasterJobStatus.InBucket;
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
        
        var jobProcessDeadline = this.GetSafeNextPlanExecutionAt().Add(processDeadlineDuration.Value);
        if (this.GetSafeNextPlanExecutionAt() < DateTime.UtcNow)
        {
            jobProcessDeadline = DateTime.UtcNow.Add(processDeadlineDuration.Value);
        }
        
        this.ProcessDeadline = jobProcessDeadline;
    }
    
    public void MarkAsHeldOnMaster()
    {
        if (this.NextPlanExecutionAt is null)
        {
            this.NextPlanExecutionAt = this.ScheduledAt > DateTime.UtcNow ? this.ScheduledAt : DateTime.UtcNow;
        }
        
        AgentConnectionId = null;
        BucketId = null;
        AgentWorkerId = null;
        ProcessDeadline = null;
        PartitionLockId = null;
        PartitionLockExpiresAt = null;
        Status = JobMasterJobStatus.OnMaster;
    } 

    public bool IsOnBoarding(TimeSpan? extraWindow = null)
    {
        var now = DateTime.UtcNow;
        var window = JobMasterConstants.ClockSkewPadding + JobMasterConstants.OnBoardingWindow;

        if (extraWindow.HasValue)
        {
            window += extraWindow.Value;
        }

        return GetSafeNextPlanExecutionAt() <= now.Add(window);
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
            NextPlanExecutionAt = null;
            FinalizedAt = DateTime.UtcNow;
            return true;
        }
        
        return false;
    }
    
    public void DelayNextExecutionPlan(TimeSpan delay)
    {
        var baseDate = ScheduledAt > DateTime.UtcNow ? ScheduledAt : DateTime.UtcNow;
        NextPlanExecutionAt = baseDate.Add(delay);
    }

    public void AdvanceNextExecutionPlan(TimeSpan advance)
    {
        var advanced = GetSafeNextPlanExecutionAt() - advance;
        NextPlanExecutionAt = advanced > ScheduledAt ? advanced : ScheduledAt;
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
        
        DelayNextExecutionPlan(timeToWait);
        ProcessDeadline = null;
        Status = JobMasterJobStatus.OnMaster;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        NumberOfFailures++;
        FinalizedAt = DateTime.UtcNow;
        return true;
    }
    
    public void MarkAsFailed()
    {
        Status = JobMasterJobStatus.Failed;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        ProcessDeadline = null;
        NextPlanExecutionAt = null;
        NumberOfFailures++;
        FinalizedAt = DateTime.UtcNow;
    }
    
    public void MarkAsSucceeded()
    {
        Status = JobMasterJobStatus.Succeeded;
        FinalizedAt = DateTime.UtcNow;
        AgentConnectionId = null;
        AgentWorkerId = null;
        BucketId = null;
        ProcessDeadline = null;
        NextPlanExecutionAt = null;
    }
    
    public void ReSchedule(DateTime scheduledAt)
    {
        if (!this.Status.IsFinalStatus())
        {
            throw new ArgumentException("Job must be succeeded, failed or cancelled.");
        }
        
        if (this.Status == JobMasterJobStatus.Processing)
        {
            throw new ArgumentException("Job is already running.");
        }
        
        this.Status = JobMasterJobStatus.OnMaster;
        this.ScheduledAt = scheduledAt;
        this.NextPlanExecutionAt = scheduledAt;
        this.AgentConnectionId = null;
        this.BucketId = null;
        this.AgentWorkerId = null;
        this.ProcessDeadline = null;
        this.NumberOfFailures = 0;
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

    public JobExecution ProcessingStarted()
    {
        Status = JobMasterJobStatus.Processing;
        ProcessStartedAt = DateTime.UtcNow;
        RefreshDeadline(Timeout + JobMasterConstants.JobProcessDeadlineDuration);

        return new JobExecution(this.ClusterId)
        {
            JobId = this.Id,
            StartedAt = DateTime.UtcNow,
            AgentConnectionId = this.AgentConnectionId!,
            AgentWorkerId = this.AgentWorkerId!,
            BucketId = this.BucketId!,
            HostId = this.HostId!,
            Outcome = JobExecutionOutcomeStatus.Processing,
        };
    }
    
    public bool ExceedProcessDeadline()
    {
        return ProcessDeadline.HasValue && ProcessDeadline.Value < DateTime.UtcNow.Add(JobMasterConstants.ClockSkewPadding).Add(TimeSpan.FromMinutes(1));
    }
    
    public bool CanHeldOnMasterExceedDeadline()
    {
        if (this.Status == JobMasterJobStatus.OnMaster)
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
    
    public static JobRawModel RecoverFromDb(JobPersistenceRecord d)
        => JobConvertUtil.FromPersistence(d);

    public static JobPersistenceRecord ToPersistence(JobRawModel m)
        => JobConvertUtil.ToPersistence(m);
}