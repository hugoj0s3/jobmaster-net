namespace JobMaster.Contracts.RecurrenceExpressions;

public interface IRecurrenceExprCompiler
{
    public string ExpressionTypeId { get; }
    IRecurrenceCompiledExpr? TryCompile(string expression);
    IRecurrenceCompiledExpr Compile(string expression);
}