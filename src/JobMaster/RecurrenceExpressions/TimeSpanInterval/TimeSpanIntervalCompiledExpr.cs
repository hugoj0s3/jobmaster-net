using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.RecurrenceExpressions.TimeSpanInterval;

/// <summary>A compiled recurrence expression that fires at a fixed <see cref="Interval"/> after each occurrence.</summary>
public sealed class TimeSpanIntervalCompiledExpr : IRecurrenceCompiledExpr
{
    /// <summary>The raw <see cref="TimeSpan"/> string used to produce this expression.</summary>
    public string Expression { get; internal set; } = string.Empty;

    /// <summary>Returns the <c>"TimeSpanInterval"</c> type identifier.</summary>
    public string ExpressionTypeId => TimeSpanIntervalExprCompiler.TypeId;

    /// <summary>The fixed duration between consecutive occurrences.</summary>
    public TimeSpan Interval { get; internal set; }

    /// <summary>Returns <paramref name="dateTime"/> offset by <see cref="Interval"/>.</summary>
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        var nextOccurrence = dateTime.Add(Interval);
        return nextOccurrence;
    }

    /// <summary>Always returns <c>false</c> — a fixed-interval expression never ends.</summary>
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return false;
    }
}