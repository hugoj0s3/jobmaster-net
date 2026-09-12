namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// Represents a compiled, executable recurrence expression produced by an <see cref="IRecurrenceExprCompiler"/>.
/// Use <see cref="GetNextOccurrence"/> to advance the schedule and <see cref="HasEnded"/> to check termination.
/// </summary>
public interface IRecurrenceCompiledExpr
{
    /// <summary>The raw recurrence expression string.</summary>
    public string Expression { get; }

    /// <summary>Identifies the compiler type that produced this instance (e.g. <c>"Cron"</c>).</summary>
    public string ExpressionTypeId { get; }

    /// <summary>
    /// Returns the next occurrence of the recurrence expression after <paramref name="dateTime"/>.
    /// Returns <c>null</c> when there are no further occurrences (the schedule has ended).
    /// </summary>
    /// <param name="dateTime">Reference point in time, expressed in <paramref name="ianaTimeZoneId"/>.</param>
    /// <param name="ianaTimeZoneId">IANA timezone identifier used to interpret <paramref name="dateTime"/>.</param>
    DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId);

    /// <summary>
    /// Returns <c>true</c> if the recurrence will never fire again relative to <paramref name="dateTime"/>.
    /// </summary>
    /// <param name="dateTime">Reference point in time.</param>
    /// <param name="ianaTimeZoneId">IANA timezone identifier used to interpret <paramref name="dateTime"/>.</param>
    bool HasEnded(DateTime dateTime, string ianaTimeZoneId);
}
