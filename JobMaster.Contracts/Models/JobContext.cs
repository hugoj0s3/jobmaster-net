namespace JobMaster.Contracts.Models;

public class JobContext 
{
    public Guid Id { get; internal set; }
    public string ClusterId { get; internal set; } = string.Empty;
    public DateTime CreatedAt { get; internal set; }
    public DateTime ScheduledAt { get; internal set; }
    public JobMasterPriority Priority { get; internal set;}
    public string JobDefinitionId { get; internal set; } = string.Empty;
    public  int NumberOfFailures { get; internal set; } 
    public TimeSpan Timeout { get; internal set; }
    public int MaxNumberOfRetries { get; internal set; }
    public IReadableMessageData MsgData { get; internal set; } = null!;
    public IReadableMetadata Metadata { get; internal set; } = null!;
    public Guid? RecurringScheduleId { get; internal set; }
    public JobSchedulingSourceType ScheduleSourceType { get; internal set; }
    public RecurringScheduleContext? RecurringSchedule { get; internal set; }
    
    public string? WorkerLane { get; internal set; }
}