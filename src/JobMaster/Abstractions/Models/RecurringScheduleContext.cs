using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions.Models;

public class RecurringScheduleContext
{
    public Guid Id { get; internal set; }
    
    public string ClusterId { get; internal set; } = string.Empty;
    
    public string? ProfileId { get; internal set; }
    
    public DateTime CreatedAt { get; internal set; }
    
    public RecurringScheduleType RecurringScheduleType { get; internal set; }
    
    public string? StaticDefinitionId { get; internal set; }
    
    public IRecurrenceCompiledExpr RecurExpression { get; internal set; } = new NeverRecursCompiledExpr();
    
    public string JobDefinitionId { get; internal set; } = string.Empty;
    
    public DateTime? StartAfter { get; internal set; }
    public DateTime? EndBefore { get; internal set; }
    
    public IReadableMetadata Metadata { get; internal set; } = null!;
    
    public string? WorkerLane { get; internal set; }
}