namespace JobMaster.Abstractions.RecurrenceExpressions;

/// <summary>
/// A recurrence compiler that always produces a <see cref="NeverRecursCompiledExpr"/>.
/// Serves as both the default expression type for new recurring schedules (null-object pattern)
/// and the termination sentinel — storing this type ID on a schedule permanently stops it
/// from generating further jobs without deleting the record.
/// </summary>
public sealed class NeverRecursExprCompiler : IRecurrenceExprCompiler
{
    /// <summary>The expression type identifier for the never-recurs sentinel: <c>"Never-Recurs"</c>.</summary>
    public const string TypeId = "Never-Recurs";

    /// <summary>Returns <see cref="TypeId"/>.</summary>
    public string ExpressionTypeId => TypeId;

    /// <summary>Returns a <see cref="NeverRecursCompiledExpr"/> regardless of the expression string.</summary>
    public IRecurrenceCompiledExpr Compile(string expression)
    {
        return new NeverRecursCompiledExpr();
    }

    /// <summary>Returns a <see cref="NeverRecursCompiledExpr"/> regardless of the expression string.</summary>
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        return Compile(expression);
    }
}
