using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Microsoft.AspNetCore.Mvc;

namespace JobMaster.Api.ApiModels;

public class ApiJobQueryCriteria
{
    [FromQuery]
    public JobMasterJobStatus? Status { get; set; }

    [FromQuery]
    public JobMasterJobStatus[]? Statuses { get; set; }

    [FromQuery]
    public JobMasterPriority? Priority { get; set; }
    
    [FromQuery]
    public DateTime? ScheduledTo { get; set; }

    [FromQuery]
    public DateTime? ScheduledFrom { get; set; }

    [FromQuery]
    public DateTime? ProcessDeadlineTo { get; set; }

    [FromQuery]
    public JobMasterTriggerSourceType[]? TriggerSourceTypes { get; set; }

    [FromQuery]
    public string? SourceId { get; set; }
    
    [FromQuery]
    public string? JobDefinitionId { get; set; }
   
    
    [FromQuery]
    public string? WorkerId { get; set; }

    [FromQuery]
    public string? AgentConnectionId { get; set; }

    [FromQuery]
    public string? HostId { get; set; }

    [FromQuery]
    public string? BucketId { get; set; }

    [FromQuery]
    public string? WorkerLane { get; set; }
    
    [FromQuery]
    public int? CountLimit { get; set; } = 100;

    [FromQuery]
    public int? Offset { get; set; }

    [FromQuery]
    public string? MetadataFiltersJson { get; set; }
    
    [FromQuery]
    public string? OrderByProperty { get; set; }
    
    [FromQuery]
    public bool? OrderByAsc { get; set; }
   
    internal JobQueryCriteria ToDomainCriteria()
    {
        Guid? sourceId = null;
        if (!string.IsNullOrWhiteSpace(SourceId))
        {
            sourceId = SourceId!.FromBase64();
        }

        var triggerSourceTypes = (TriggerSourceTypes ?? Array.Empty<JobMasterTriggerSourceType>()).ToList();
        var statuses = (Statuses ?? Array.Empty<JobMasterJobStatus>()).ToList();

        var sortByCriteria = !string.IsNullOrEmpty(this.OrderByProperty) ? 
            new SortByCriteria { 
                Property = OrderByProperty, 
                Ascending = OrderByAsc ?? true,
            } : null;
        
        return new JobQueryCriteria
        {
            Status = Status,
            ScheduledTo = ScheduledTo,
            ScheduledFrom = ScheduledFrom,
            ProcessDeadlineTo = ProcessDeadlineTo,
            TriggerSourceTypes = triggerSourceTypes,
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
            Statuses = statuses,
            Priority = Priority,
            SortBy = sortByCriteria,
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
        };
    }
}
