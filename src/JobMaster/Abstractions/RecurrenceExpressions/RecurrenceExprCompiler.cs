namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// Entry point for compiling recurrence expressions by type identifier.
/// Delegates to the <see cref="RecurrenceCompilerFactory"/> to resolve the correct
/// <see cref="IRecurrenceExprCompiler"/> for the given type.
/// </summary>
public static class RecurrenceExprCompiler
{
    /// <summary>
    /// Resolves the compiler for <paramref name="recurrenceTypeId"/> and compiles <paramref name="expression"/>.
    /// Throws <see cref="ArgumentException"/> if the type is unknown or the expression is invalid.
    /// </summary>
    public static IRecurrenceCompiledExpr Compile(string recurrenceTypeId, string expression)
    {
        var compiler = RecurrenceCompilerFactory.GetCompiler(recurrenceTypeId);
        if (compiler == null)
        {
            throw new ArgumentException($"Unknown recurrence type: {recurrenceTypeId}");
        }

        return compiler.Compile(expression);
    }

    /// <summary>
    /// Resolves the compiler for <paramref name="recurrenceTypeId"/> and attempts to compile
    /// <paramref name="expression"/>. Returns <c>null</c> if the type is unknown or the expression is invalid.
    /// </summary>
    public static IRecurrenceCompiledExpr? TryCompile(string recurrenceTypeId, string expression)
    {
        var compiler = RecurrenceCompilerFactory.TryGetCompiler(recurrenceTypeId);
        if (compiler == null)
            return null;

        return compiler.TryCompile(expression);
    }
}
