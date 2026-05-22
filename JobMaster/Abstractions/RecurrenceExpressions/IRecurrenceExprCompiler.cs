namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// Compiles recurrence expression strings into executable <see cref="IRecurrenceCompiledExpr"/> instances.
/// Implement this interface to add support for a custom recurrence type and register it via
/// <see cref="RecurrenceCompilerFactory.RegisterCompiler"/>.
/// </summary>
public interface IRecurrenceExprCompiler
{
    /// <summary>
    /// A unique identifier for the recurrence expression type this compiler handles (e.g. <c>"Cron"</c>).
    /// Must match the type ID stored on the schedule.
    /// </summary>
    public string ExpressionTypeId { get; }

    /// <summary>
    /// Attempts to compile <paramref name="expression"/>. Returns <c>null</c> if the expression is invalid
    /// rather than throwing.
    /// </summary>
    IRecurrenceCompiledExpr? TryCompile(string expression);

    /// <summary>
    /// Compiles <paramref name="expression"/> and returns the result.
    /// Throws if the expression is invalid.
    /// </summary>
    IRecurrenceCompiledExpr Compile(string expression);
}
