using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.RecurrenceExpressions.TimeSpanInterval;

public sealed class TimeSpanIntervalCompiledExpr : IRecurrenceCompiledExpr
{
    public string Expression { get; internal set; } = string.Empty;
    
    public string ExpressionTypeId => TimeSpanIntervalExprCompiler.TypeId;
    
    public TimeSpan Interval { get; internal set; }
    
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        var nextOccurrence = dateTime.Add(Interval);
        return nextOccurrence;
    }
    
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return false;
    }
}