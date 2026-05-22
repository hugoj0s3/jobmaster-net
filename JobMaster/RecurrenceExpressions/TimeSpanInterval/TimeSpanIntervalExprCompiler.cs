using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.RecurrenceExpressions.TimeSpanInterval;

/// <summary>
/// Compiles fixed-interval recurrence expressions from a <see cref="TimeSpan"/> string (e.g. "00:05:00" for every 5 minutes).
/// Registered automatically under the type ID <c>"TimeSpanInterval"</c>.
/// </summary>
public sealed class TimeSpanIntervalExprCompiler : IRecurrenceExprCompiler
{
    /// <summary>The expression type identifier for time-span interval recurrence: <c>"TimeSpanInterval"</c>.</summary>
    public const string TypeId = "TimeSpanInterval";

    /// <summary>Returns <see cref="TypeId"/>.</summary>
    public string ExpressionTypeId => TypeId;

    /// <summary>Attempts to compile <paramref name="expression"/> as a <see cref="TimeSpan"/>. Returns <c>null</c> if the format is invalid.</summary>
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        var (_, expr) = TryCompileInternal(expression);
        return expr;
    }

    /// <summary>Compiles <paramref name="expression"/> as a <see cref="TimeSpan"/>. Throws <see cref="ArgumentException"/> if the format is invalid.</summary>
    public IRecurrenceCompiledExpr Compile(string expression)
    {
        var (error, expr) = TryCompileInternal(expression);
        if (expr == null)
            throw new ArgumentException(error ?? "Unknown error compiling interval expression.", nameof(expression));
        return expr;
    }

    /// <summary>Creates a compiled expression directly from a <see cref="TimeSpan"/> value.</summary>
    public static TimeSpanIntervalCompiledExpr FromInterval(TimeSpan interval)
    {
        return (TimeSpanIntervalCompiledExpr) RecurrenceExprCompiler.Compile(TypeId, interval.ToString());
    }

    private (string? error, IRecurrenceCompiledExpr? expr) TryCompileInternal(string expression)
    {
        if (string.IsNullOrEmpty(expression))
            return ("Interval expression cannot be empty.", null);

        if (!TimeSpan.TryParse(expression, out var interval))
            return ($"Invalid interval format: '{expression}'. Use format HH:mm:ss or d.HH:mm:ss.", null);

        if (interval.TotalSeconds < 1)
            return ("Interval must be at least 1 second.", null);

        return (null, new TimeSpanIntervalCompiledExpr
        {
            Expression = expression,
            Interval = interval
        });
    }
}