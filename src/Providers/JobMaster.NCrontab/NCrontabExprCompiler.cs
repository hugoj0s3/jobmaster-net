using JobMaster.Abstractions.RecurrenceExpressions;
using NCrontab;

namespace JobMaster.NCrontab;

/// <summary>Compiles standard cron expressions via <see cref="NCrontab.CrontabSchedule"/>.</summary>
public class NCrontabExprCompiler : IRecurrenceExprCompiler
{
    public const string TypeId = "NCrontab";
    public string ExpressionTypeId => TypeId;

    /// <summary>Attempts to compile <paramref name="expression"/>, returning <c>null</c> if it is invalid.</summary>
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        var schedule = CrontabSchedule.TryParse(expression, DetectParseOptions(expression));
        return schedule is null ? null : new NCrontabCompiledExpr(expression, schedule);
    }

    /// <summary>Compiles <paramref name="expression"/>, throwing if it is invalid.</summary>
    public IRecurrenceCompiledExpr Compile(string expression)
    {
        var schedule = CrontabSchedule.Parse(expression, DetectParseOptions(expression));
        return new NCrontabCompiledExpr(expression, schedule);
    }

    /// <summary>
    /// Detects 5-field (standard) vs 6-field (seconds-precision) format by counting whitespace-separated
    /// tokens -- <see cref="IRecurrenceExprCompiler"/> has no room for an explicit options parameter, since it's
    /// shared with the generic expression-type-id + string scheduling path.
    /// </summary>
    private static CrontabSchedule.ParseOptions DetectParseOptions(string expression)
    {
        var tokenCount = expression.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;
        return new CrontabSchedule.ParseOptions { IncludingSeconds = tokenCount >= 6 };
    }
}
