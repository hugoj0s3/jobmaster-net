namespace JobMaster.Contracts.RecurrenceExpressions;

public static class RecurrenceExprCompiler
{
    public static IRecurrenceCompiledExpr Compile(string recurrenceTypeId, string expression)
    {
        var compiler = RecurrenceCompilerFactory.GetCompiler(recurrenceTypeId);
        if (compiler == null)
        {
            throw new ArgumentException($"Unknown recurrence type: {recurrenceTypeId}");
        }
            
        
        return compiler.Compile(expression);
    }
    
    public static IRecurrenceCompiledExpr? TryCompile(string recurrenceTypeId, string expression)
    {
        var compiler = RecurrenceCompilerFactory.TryGetCompiler(recurrenceTypeId);
        if (compiler == null)
            return null;
        
        return compiler.TryCompile(expression);
    }
}