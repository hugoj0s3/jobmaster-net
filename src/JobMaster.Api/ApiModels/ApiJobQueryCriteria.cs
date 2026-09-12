using JobMaster.Abstractions.Models;
using JobMaster.Api.Endpoints;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Microsoft.AspNetCore.Mvc;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for filtering and paginating jobs.</summary>
public class ApiJobQueryCriteria
{
    /// <summary>Filter by a single job status.</summary>
    public JobMasterJobStatus? Status { get; set; }
    /// <summary>Filter by multiple job statuses (OR condition).</summary>
    public JobMasterJobStatus[]? Statuses { get; set; }
    /// <summary>Filter by job priority.</summary>
    public JobMasterPriority? Priority { get; set; }
    /// <summary>Upper bound for the next planned execution window.</summary>
    public DateTime? NextPlanExecutionAtTo { get; set; }
    /// <summary>Lower bound for the next planned execution window.</summary>
    public DateTime? NextPlanExecutionAtFrom { get; set; }
    /// <summary>Lower bound for the scheduled execution time.</summary>
    public DateTime? ScheduledFrom { get; set; }
    /// <summary>Upper bound for the scheduled execution time.</summary>
    public DateTime? ScheduledTo { get; set; }
    /// <summary>Upper bound for the process deadline.</summary>
    public DateTime? ProcessDeadlineTo { get; set; }
    /// <summary>Filter by one or more trigger source types.</summary>
    public JobMasterTriggerSourceType[]? TriggerSourceTypes { get; set; }
    /// <summary>Filter by multiple source IDs (base64url-encoded or standard GUID format).</summary>
    public string[]? SourceIds { get; set; }
    /// <summary>Filter by a single source ID (base64url-encoded or standard GUID format).</summary>
    public string? SourceId { get; set; }
    /// <summary>Filter by job definition ID.</summary>
    public string? JobDefinitionId { get; set; }
    /// <summary>Filter by worker ID.</summary>
    public string? WorkerId { get; set; }
    /// <summary>Filter by agent connection ID.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>Filter by host ID.</summary>
    public string? HostId { get; set; }
    /// <summary>Filter by bucket ID.</summary>
    public string? BucketId { get; set; }
    /// <summary>Filter by worker lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>Maximum number of results to return. Defaults to 25.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
    public int? Offset { get; set; }
    /// <summary>JSON-encoded list of metadata filters to apply.</summary>
    public string? MetadataFiltersJson { get; set; }
    /// <summary>Optional sort specification.</summary>
    public ApiSortByCriteria? SortBy { get; set; } 
   
    internal JobQueryCriteria ToDomainCriteria()
    {
        Guid? sourceId = null;
        if (!string.IsNullOrWhiteSpace(SourceId))
        {
            sourceId = SourceId!.ParseFlexible();
        }

        var sourceIds = (SourceIds ?? Array.Empty<string>()).Select(s => s.ParseFlexible()).ToArray();

        var triggerSourceTypes = (TriggerSourceTypes ?? Array.Empty<JobMasterTriggerSourceType>()).ToList();
        
        var statuses = (Statuses ?? Array.Empty<JobMasterJobStatus>()).ToList();

        // Validate sort property if provided
        if (SortBy != null && !string.IsNullOrWhiteSpace(SortBy.Property))
        {
            EndpointSortingUtil.ValidateSortingProperty<ApiJobModel>(SortBy.Property);
        }

        return new JobQueryCriteria
        {
            Status = Status,
            Statuses = statuses,
            Priority = Priority,
            NextPlanExecutionAtTo = NextPlanExecutionAtTo,
            NextPlanExecutionAtFrom = NextPlanExecutionAtFrom,
            ScheduledFrom = ScheduledFrom,
            ScheduledTo = ScheduledTo,
            ProcessDeadlineTo = ProcessDeadlineTo,
            TriggerSourceTypes = triggerSourceTypes,
            SourceIds = sourceIds,
            SourceId = sourceId,
            MetadataFilters = ApiGenericRecordValueFilterMappings.ParseMetadataFiltersJson(MetadataFiltersJson),
            JobDefinitionId = JobDefinitionId,
            WorkerLane = WorkerLane,
            CountLimit = CountLimit ?? 25,
            Offset = Offset ?? 0,
            AgentConnectionId = AgentConnectionId,
            HostId = HostId,
            BucketId = BucketId,
            WorkerId = WorkerId,
            SortBy = SortBy != null && !string.IsNullOrWhiteSpace(SortBy.Property)
                ? new SortByCriteria { Property = SortBy.Property, Ascending = SortBy.Ascending }
                : null,
        };
    }
}
