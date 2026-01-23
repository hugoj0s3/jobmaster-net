using JobMaster.Abstractions.RecurrenceExpressions;
using NaturalCron;

namespace JobMaster.RecurrenceExpressions.NaturalCron;

public class NaturalCronCompiledExpr : IRecurrenceCompiledExpr
{
    private readonly NaturalCronExpr naturalCronExpr;
    public string Expression { get; internal set; }
    public string ExpressionTypeId => NaturalCronExprCompiler.TypeId;
    
    public NaturalCronCompiledExpr(string expression, NaturalCronExpr naturalCronExpr)
    {
        this.naturalCronExpr = naturalCronExpr;
        Expression = expression;
    }
    
    public DateTime? GetNextOccurrence(DateTime dateTime, string ianaTimeZoneId)
    {
        return naturalCronExpr.TryGetNextOccurrenceInTz(dateTime, ianaTimeZoneId);
    }

    public bool HasEnded(DateTime dateTime, string ianaTimeZoneId)
    {
        return naturalCronExpr.TryGetNextOccurrenceInTz(dateTime, ianaTimeZoneId) == null;
    }
}