using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

public class StaticRecurringScheduleDefinition
{
    internal StaticRecurringScheduleDefinition(
        string clusterId,
        string jobDefinitionId, 
        IRecurrenceCompiledExpr compiledExpr, 
        string id, 
        JobMasterPriority? priority = null, 
        TimeSpan? timeout = null, 
        DateTime? startAfter = null, 
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null,
        string? workerLane = null)
    {
        ClusterId = clusterId;
        JobDefinitionId = jobDefinitionId;
        CompiledExpr = compiledExpr;
        Id = id;
        Priority = priority;
        Timeout = timeout;
        StartAfter = startAfter;
        EndBefore = endBefore;
        Metadata = metadata;
        WorkerLane = workerLane;
    }
    
    

    public string JobDefinitionId { get; private set; }
    public IRecurrenceCompiledExpr CompiledExpr { get; private set; }
    public string Id { get; private set; }
    public JobMasterPriority? Priority { get; private set; }
    public TimeSpan? Timeout { get; private set; }
    public DateTime? StartAfter { get; private set; }
    public DateTime? EndBefore { get; private set; }
    
    public string ClusterId { get; private set; }
    
    public int? MaxNumberOfRetries { get; private set; }
    public IWritableMetadata? Metadata { get; private set; }
    public string? WorkerLane { get; private set; }
}