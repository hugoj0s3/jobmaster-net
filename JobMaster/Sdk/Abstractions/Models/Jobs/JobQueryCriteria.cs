using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Sdk.Abstractions.Models.Jobs;

internal class JobQueryCriteria
{
    public JobMasterJobStatus? Status { get; set; }
    public IList<JobMasterJobStatus> Statuses { get; set; } = new List<JobMasterJobStatus>();

    public JobMasterPriority? Priority { get; set; }
    
    public DateTime? NextPlanExecutionAtTo { get; set; }
    public DateTime? NextPlanExecutionAtFrom { get; set; }
    
    
    public DateTime? ScheduledFrom { get; set; }
    public DateTime? ScheduledTo { get; set; }
    
    public DateTime? ProcessDeadlineTo { get; set; }
    public IList<JobMasterTriggerSourceType> TriggerSourceTypes { get; set; } = new List<JobMasterTriggerSourceType>();
    public IList<Guid> SourceIds { get; set; } = new List<Guid>();
    public Guid? SourceId { get; set; }
    
    public IList<GenericRecordValueFilter> MetadataFilters { get; set; } = new List<GenericRecordValueFilter>();
    public string? JobDefinitionId { get; set; }
    public string? WorkerLane { get; set; }
    
    public string? WorkerId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? HostId { get; set; }
    public string? BucketId { get; set; }
    
    public IList<Guid> JobIds { get; set; } = new List<Guid>();
    
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
    
    public SortByCriteria? SortBy { get; set; }
    
    public ReadIsolationLevel ReadIsolationLevel { get; set; } = ReadIsolationLevel.Consistent;
}