using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

public class ApiJobQueryCriteria
{
    public JobMasterJobStatus? Status { get; set; }
    public DateTime? ScheduledTo { get; set; }
    public DateTime? ScheduledFrom { get; set; }
    public DateTime? ProcessDeadlineTo { get; set; }
    public string? RecurringScheduleId { get; set; }
    public string? MetadataFiltersJson { get; set; }
    public string? JobDefinitionId { get; set; }
    public string? WorkerLane { get; set; }
    public int CountLimit { get; set; }
    public int Offset { get; set; }

    internal JobQueryCriteria ToDomainCriteria()
    {
        Guid? recurringScheduleId = null;
        if (!string.IsNullOrWhiteSpace(RecurringScheduleId))
        {
            recurringScheduleId = RecurringScheduleId!.FromBase64();
        }

        return new JobQueryCriteria
        {
            Status = Status,
            ScheduledTo = ScheduledTo,
            ScheduledFrom = ScheduledFrom,
            ProcessDeadlineTo = ProcessDeadlineTo,
            RecurringScheduleId = recurringScheduleId,
            MetadataFilters = ApiGenericRecordValueFilterMappings.ParseMetadataFiltersJson(MetadataFiltersJson),
            JobDefinitionId = JobDefinitionId,
            WorkerLane = WorkerLane,
            CountLimit = CountLimit,
            Offset = Offset,
        };
    }
}