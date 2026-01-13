using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;

namespace JobMaster.Sdk.Contracts.Models.Jobs;

public class JobQueryCriteria
{
    public JobMasterJobStatus? Status { get; set; }
    public DateTime? ScheduledTo { get; set; }
    public DateTime? ScheduledFrom { get; set; }
    public DateTime? ProcessDeadlineTo { get; set; }
    public Guid? RecurringScheduleId { get; set; }
    
    public IList<GenericRecordValueFilter> MetadataFilters { get; set; } = new List<GenericRecordValueFilter>();
    public string? JobDefinitionId { get; set; }
    public bool? IsLocked { get; set; }
    public int? PartitionLockId { get; set; }
    public string? WorkerLane { get; set; }
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
}