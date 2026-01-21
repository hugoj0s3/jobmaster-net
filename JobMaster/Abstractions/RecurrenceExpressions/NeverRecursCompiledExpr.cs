namespace JobMaster.Abstractions.RecurrenceExpressions;

public class NeverRecursCompiledExpr : IRecurrenceCompiledExpr
{
    public string Expression => string.Empty;
    public string ExpressionTypeId => NeverRecursExprCompiler.TypeId;
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        return null;
    }
    
    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return true;
    }
}