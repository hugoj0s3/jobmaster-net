using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Api.ApiModels;

public class ApiRecurringScheduleQueryCriteria
{
    public RecurringScheduleStatus? Status { get; set; }
    public DateTime? StartAfterTo { get; set; }
    public DateTime? StartAfterFrom { get; set; }
    public DateTime? EndBeforeTo { get; set; }
    public DateTime? EndBeforeFrom { get; set; }
    public DateTime? CoverageUntil { get; set; }
    public bool? IsJobCancellationPending { get; set; }
    public bool? CanceledOrInactive { get; set; }
    public RecurringScheduleType? RecurringScheduleType { get; set; }
    public string? JobDefinitionId { get; set; }
    public string? ProfileId { get; set; }
    public string? WorkerLane { get; set; }
    public string? MetadataFiltersJson { get; set; }
    
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }

    internal RecurringScheduleQueryCriteria ToDomainCriteria()
    {
        return new RecurringScheduleQueryCriteria
        {
            Status = Status,
            StartAfterTo = StartAfterTo,
            StartAfterFrom = StartAfterFrom,
            EndBeforeTo = EndBeforeTo,
            EndBeforeFrom = EndBeforeFrom,
            CoverageUntil = CoverageUntil,
            IsJobCancellationPending = IsJobCancellationPending,
            CanceledOrInactive = CanceledOrInactive,
            RecurringScheduleType = RecurringScheduleType,
            JobDefinitionId = JobDefinitionId,
            ProfileId = ProfileId,
            WorkerLane = WorkerLane,
            MetadataFilters = ApiGenericRecordValueFilterMappings.ParseMetadataFiltersJson(MetadataFiltersJson),
            CountLimit = CountLimit,
            Offset = Offset,
        };
    }
}