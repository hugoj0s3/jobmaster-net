namespace JobMaster.Contracts.RecurrenceExpressions;

public sealed class NeverRecursExprCompiler : IRecurrenceExprCompiler
{
    public const string TypeId = "Never-Recurs";
    public string ExpressionTypeId => TypeId;

    public IRecurrenceCompiledExpr Compile(string expression)
    {
        return new NeverRecursCompiledExpr();
    }

    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        return Compile(expression);
    }
}