using JobMaster.Abstractions.RecurrenceExpressions;
using NaturalCron;

namespace JobMaster.RecurrenceExpressions.NaturalCron;

/// <summary>
/// Compiles NaturalCron recurrence expressions (human-readable cron-like syntax).
/// Registered automatically under the type ID <c>"NaturalCron"</c>.
/// </summary>
public class NaturalCronExprCompiler : IRecurrenceExprCompiler
{
    /// <summary>The expression type identifier for NaturalCron recurrence: <c>"NaturalCron"</c>.</summary>
    public const string TypeId = "NaturalCron";

    /// <summary>Returns <see cref="TypeId"/>.</summary>
    public string ExpressionTypeId => TypeId;

    /// <summary>Attempts to parse <paramref name="expression"/> as a NaturalCron expression. Returns <c>null</c> if parsing fails.</summary>
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        var (naturalCronExpr, errors) = NaturalCronExpr.TryParse(expression);
        if (errors.Any() || naturalCronExpr is null)
            return null;
        
        return new NaturalCronCompiledExpr(expression, naturalCronExpr);
    }

    /// <summary>Parses <paramref name="expression"/> as a NaturalCron expression. Throws if parsing fails.</summary>
    public IRecurrenceCompiledExpr Compile(string expression)
    {
        NaturalCronExpr naturalCronExpr = NaturalCronExpr.Parse(expression);
        return new NaturalCronCompiledExpr(expression, naturalCronExpr);
    }
}