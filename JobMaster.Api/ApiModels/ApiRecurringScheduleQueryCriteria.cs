using JobMaster.Abstractions.Models;
using JobMaster.Api.Endpoints;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for filtering and paginating recurring schedules.</summary>
public class ApiRecurringScheduleQueryCriteria
{
    /// <summary>Filter by schedule lifecycle status.</summary>
    public RecurringScheduleStatus? Status { get; set; }
    /// <summary>Upper bound for the start-after date.</summary>
    public DateTime? StartAfterTo { get; set; }
    /// <summary>Lower bound for the start-after date.</summary>
    public DateTime? StartAfterFrom { get; set; }
    /// <summary>Upper bound for the end-before date.</summary>
    public DateTime? EndBeforeTo { get; set; }
    /// <summary>Lower bound for the end-before date.</summary>
    public DateTime? EndBeforeFrom { get; set; }
    /// <summary>Filter schedules whose plan coverage does not extend past this date.</summary>
    public DateTime? CoverageUntil { get; set; }
    /// <summary>Filter schedules with a pending job cancellation.</summary>
    public bool? IsJobCancellationPending { get; set; }
    /// <summary>Filter schedules that are canceled or inactive.</summary>
    public bool? CanceledOrInactive { get; set; }
    /// <summary>Filter by schedule type (static or dynamic).</summary>
    public RecurringScheduleType? RecurringScheduleType { get; set; }
    /// <summary>Filter by job definition ID.</summary>
    public string? JobDefinitionId { get; set; }
    /// <summary>Filter by static profile ID.</summary>
    public string? ProfileId { get; set; }
    /// <summary>Filter by worker lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>JSON-encoded list of metadata filters to apply.</summary>
    public string? MetadataFiltersJson { get; set; }
    /// <summary>Maximum number of results to return. Defaults to 25.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
    public int? Offset { get; set; }
    /// <summary>Optional sort specification.</summary>
    public ApiSortByCriteria? SortBy { get; set; }

    internal RecurringScheduleQueryCriteria ToDomainCriteria()
    {
        // Validate sort property if provided
        if (SortBy != null && !string.IsNullOrWhiteSpace(SortBy.Property))
        {
            EndpointSortingUtil.ValidateSortingProperty<ApiRecurringScheduleModel>(SortBy.Property);
        }

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
            CountLimit = CountLimit ?? 25,
            Offset = Offset ?? 0,
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
            SortBy = SortBy != null && !string.IsNullOrWhiteSpace(SortBy.Property) 
                ? new SortByCriteria { Property = SortBy.Property, Ascending = SortBy.Ascending } 
                : null,
        };
    }
}
