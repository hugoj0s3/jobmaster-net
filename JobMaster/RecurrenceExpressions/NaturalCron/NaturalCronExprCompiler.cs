using JobMaster.Abstractions.RecurrenceExpressions;
using NaturalCron;

namespace JobMaster.RecurrenceExpressions.NaturalCron;

public class NaturalCronExprCompiler : IRecurrenceExprCompiler
{
    public const string TypeId = "NaturalCron";
    
    public string ExpressionTypeId => TypeId;
    
    public IRecurrenceCompiledExpr? TryCompile(string expression)
    {
        var (naturalCronExpr, errors) = NaturalCronExpr.TryParse(expression);
        if (errors.Any() || naturalCronExpr is null)
            return null;
        
        return new NaturalCronCompiledExpr(expression, naturalCronExpr);
    }

    public IRecurrenceCompiledExpr Compile(string expression)
    {
        NaturalCronExpr naturalCronExpr = NaturalCronExpr.Parse(expression);
        return new NaturalCronCompiledExpr(expression, naturalCronExpr);
    }
}