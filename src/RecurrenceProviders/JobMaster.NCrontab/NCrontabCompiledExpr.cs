using JobMaster.Abstractions.RecurrenceExpressions;
using NCrontab;

namespace JobMaster.NCrontab;

/// <summary>A compiled NCrontab recurrence expression that delegates occurrence calculation to the underlying <see cref="CrontabSchedule"/>.</summary>
public class NCrontabCompiledExpr : IRecurrenceCompiledExpr
{
    private readonly CrontabSchedule schedule;

    /// <summary>The raw cron expression string.</summary>
    public string Expression { get; internal set; }

    /// <summary>Returns the <c>"NCrontab"</c> type identifier.</summary>
    public string ExpressionTypeId => NCrontabExprCompiler.TypeId;

    /// <summary>Initializes a compiled NCrontab expression from a parsed <see cref="CrontabSchedule"/>.</summary>
    public NCrontabCompiledExpr(string expression, CrontabSchedule schedule)
    {
        this.schedule = schedule;
        Expression = expression;
    }

    /// <summary>
    /// Returns the next occurrence after <paramref name="dateTime"/>, or <c>null</c> if the expression has no further occurrences.
    /// </summary>
    /// <remarks>
    /// <paramref name="dateTime"/> arrives as a wall-clock value already localized to <paramref name="ianaTimeZoneId"/> by the
    /// caller (<c>RecurringSchedulePlanner</c>) -- this method must not do its own timezone conversion, so
    /// <paramref name="ianaTimeZoneId"/> is unused. NCrontab does pure wall-clock field arithmetic regardless of
    /// <see cref="DateTime.Kind"/>, so <paramref name="dateTime"/> is passed through unchanged. NCrontab's
    /// <c>GetNextOccurrence(DateTime)</c> is internally bounded (equivalent to searching up to
    /// <see cref="DateTime.MaxValue"/>) and returns <see cref="DateTime.MaxValue"/> itself -- not an exception --
    /// when no further occurrence exists, which is mapped to <c>null</c> here.
    /// </remarks>
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        var next = schedule.GetNextOccurrence(dateTime);
        return next == DateTime.MaxValue ? null : next;
    }

    /// <summary>Returns <c>true</c> when the expression has no next occurrence after <paramref name="dateTime"/>.</summary>
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return GetNextOccurrence(dateTime, ianaTimeZoneId) == null;
    }
}
