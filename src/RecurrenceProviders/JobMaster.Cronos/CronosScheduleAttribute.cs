using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.Cronos;

/// <summary>Declares a static recurring schedule using a standard cron expression, compiled via Cronos.</summary>
public sealed class CronosScheduleAttribute : RecurringScheduleAttribute
{
    public CronosScheduleAttribute(string expression) : base(expression) { }

    public override string ExpressionTypeId => CronosExprCompiler.TypeId;
}
