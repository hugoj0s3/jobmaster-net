using Cronos;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Cronos;

/// <summary>A compiled Cronos recurrence expression that delegates occurrence calculation to the underlying <see cref="CronExpression"/>.</summary>
public class CronosCompiledExpr : IRecurrenceCompiledExpr
{
    private readonly CronExpression cronExpression;

    /// <summary>The raw cron expression string.</summary>
    public string Expression { get; internal set; }

    /// <summary>Returns the <c>"Cronos"</c> type identifier.</summary>
    public string ExpressionTypeId => CronosExprCompiler.TypeId;

    /// <summary>Initializes a compiled Cronos expression from a parsed <see cref="CronExpression"/>.</summary>
    public CronosCompiledExpr(string expression, CronExpression cronExpression)
    {
        this.cronExpression = cronExpression;
        Expression = expression;
    }

    /// <summary>
    /// Returns the next occurrence after <paramref name="dateTime"/>, or <c>null</c> if the expression has no further occurrences.
    /// </summary>
    /// <remarks>
    /// <paramref name="dateTime"/> arrives as a wall-clock value already localized to <paramref name="ianaTimeZoneId"/> by the
    /// caller (<c>RecurringSchedulePlanner</c>) -- this method must not do its own timezone conversion, so
    /// <paramref name="ianaTimeZoneId"/> is unused. Cronos requires <see cref="DateTimeKind.Utc"/> on its input, so the
    /// wall-clock value is relabeled (not converted) to satisfy that check, and the result relabeled back to
    /// <see cref="DateTimeKind.Unspecified"/> to match the wall-clock contract.
    /// </remarks>
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        var asUtc = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
        var next = cronExpression.GetNextOccurrence(asUtc);
        return next.HasValue ? DateTime.SpecifyKind(next.Value, DateTimeKind.Unspecified) : null;
    }

    /// <summary>Returns <c>true</c> when the expression has no next occurrence after <paramref name="dateTime"/>.</summary>
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return GetNextOccurrence(dateTime, ianaTimeZoneId) == null;
    }
}
