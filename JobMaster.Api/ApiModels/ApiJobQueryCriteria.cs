using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Microsoft.AspNetCore.Mvc;

namespace JobMaster.Api.ApiModels;

public class ApiJobQueryCriteria
{
    public JobMasterJobStatus? Status { get; set; }
   
    public JobMasterJobStatus[]? Statuses { get; set; }

    public JobMasterPriority? Priority { get; set; }
    
    public DateTime? ScheduledTo { get; set; }
    public DateTime? ScheduledFrom { get; set; }
    public DateTime? ProcessDeadlineTo { get; set; }
    public JobMasterTriggerSourceType[]? TriggerSourceTypes { get; set; }
    public string? SourceId { get; set; }
    
    public string? JobDefinitionId { get; set; }
   
    
    public string? WorkerId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? HostId { get; set; }
    public string? BucketId { get; set; }

    public string? WorkerLane { get; set; }
    
    public int? CountLimit { get; set; } = 100;
    public int? Offset { get; set; }
    public string? MetadataFiltersJson { get; set; } 
   
    internal JobQueryCriteria ToDomainCriteria()
    {
        Guid? sourceId = null;
        if (!string.IsNullOrWhiteSpace(SourceId))
        {
            sourceId = SourceId!.FromBase64();
        }

        var triggerSourceTypes = (TriggerSourceTypes ?? Array.Empty<JobMasterTriggerSourceType>()).ToList();

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
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
        };
    }
}
