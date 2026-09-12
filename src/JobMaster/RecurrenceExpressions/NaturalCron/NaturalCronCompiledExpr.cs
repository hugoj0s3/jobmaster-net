using JobMaster.Abstractions.RecurrenceExpressions;
using NaturalCron;

namespace JobMaster.RecurrenceExpressions.NaturalCron;

/// <summary>A compiled NaturalCron recurrence expression that delegates occurrence calculation to the underlying <see cref="NaturalCronExpr"/>.</summary>
public class NaturalCronCompiledExpr : IRecurrenceCompiledExpr
{
    private readonly NaturalCronExpr naturalCronExpr;

    /// <summary>The raw NaturalCron expression string.</summary>
    public string Expression { get; internal set; }

    /// <summary>Returns the <c>"NaturalCron"</c> type identifier.</summary>
    public string ExpressionTypeId => NaturalCronExprCompiler.TypeId;

    /// <summary>Initializes a compiled NaturalCron expression from a parsed <see cref="NaturalCronExpr"/>.</summary>
    public NaturalCronCompiledExpr(string expression, NaturalCronExpr naturalCronExpr)
    {
        this.naturalCronExpr = naturalCronExpr;
        Expression = expression;
    }

    /// <summary>Returns the next occurrence after <paramref name="dateTime"/> in the given IANA time zone, or <c>null</c> if the expression has no further occurrences.</summary>
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        return naturalCronExpr.TryGetNextOccurrenceInTz(dateTime, ianaTimeZoneId);
    }

    /// <summary>Returns <c>true</c> when the expression has no next occurrence after <paramref name="dateTime"/>.</summary>
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return naturalCronExpr.TryGetNextOccurrenceInTz(dateTime, ianaTimeZoneId) == null;
    }
}