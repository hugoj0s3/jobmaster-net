using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;

namespace JobMaster.Sdk.Contracts.Models.RecurringSchedules;

public class RecurringScheduleQueryCriteria
{
    public RecurringScheduleStatus? Status { get; set; }
    
    public DateTime? StartAfterTo { get; set; }
    public DateTime? StartAfterFrom { get; set; }
    
    public DateTime? EndBeforeTo { get; set; }
    public DateTime? EndBeforeFrom { get; set; }
    public DateTime? CoverageUntil { get; set; }
    public int CountLimit { get; set; } = 100;
    
    public int Offset { get; set; }
    public bool? IsLocked { get; set; }
    public int? PartitionLockId { get; set; }
    
    public bool? IsJobCancellationPending { get; set; }
    
    public bool? CanceledOrInactive { get; set; }
    
    public RecurringScheduleType? RecurringScheduleType { get; set; }
    
    public string? JobDefinitionId { get; set; }
    
    public string? ProfileId { get; set; }
    
    public string? WorkerLane { get; set; }
    
    public IList<GenericRecordValueFilter> MetadataFilters { get; set; } = new List<GenericRecordValueFilter>();
}