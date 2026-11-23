using Cronos;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Cronos;

/// <summary>Compiles standard cron expressions via <see cref="Cronos.CronExpression"/>.</summary>
public class CronosExprCompiler : IRecurrenceExprCompiler
{
    public const string TypeId = "Cronos";
    public string ExpressionTypeId => TypeId;

    /// <summary>Attempts to compile <paramref name="expression"/>, returning <c>null</c> if it is invalid.</summary>
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        if (!CronExpression.TryParse(expression, DetectFormat(expression), out var cronExpression))
        {
            return null;
        }

        return new CronosCompiledExpr(expression, cronExpression);
    }

    /// <summary>Compiles <paramref name="expression"/>, throwing if it is invalid.</summary>
    public IRecurrenceCompiledExpr Compile(string expression)
    {
        var cronExpression = CronExpression.Parse(expression, DetectFormat(expression));
        return new CronosCompiledExpr(expression, cronExpression);
    }

    /// <summary>
    /// Detects 5-field (standard) vs 6-field (seconds-precision) format by counting whitespace-separated
    /// tokens -- <see cref="IRecurrenceExprCompiler"/> has no room for an explicit format parameter, since it's
    /// shared with the generic expression-type-id + string scheduling path.
    /// </summary>
    private static CronFormat DetectFormat(string expression)
    {
        var tokenCount = expression.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;
        return tokenCount >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;
    }
}
