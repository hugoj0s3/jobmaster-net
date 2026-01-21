using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.RecurrenceExpressions;

public sealed class TimeSpanIntervalExprCompiler : IRecurrenceExprCompiler
{
    public const string TypeId = "TimeSpanInterval";
    public string ExpressionTypeId => TypeId;

    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        var (_, expr) = TryCompileInternal(expression);
        return expr;
    }

    public IRecurrenceCompiledExpr Compile(string expression)
    {
        var (error, expr) = TryCompileInternal(expression);
        if (expr == null)
            throw new ArgumentException(error ?? "Unknown error compiling interval expression.", nameof(expression));
        return expr;
    }

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