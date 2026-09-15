using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.NCrontab;

/// <summary>Declares a static recurring schedule using a standard cron expression, compiled via NCrontab.</summary>
public sealed class NCrontabScheduleAttribute : RecurringScheduleAttribute
{
    public NCrontabScheduleAttribute(string expression) : base(expression) { }

    public override string ExpressionTypeId => NCrontabExprCompiler.TypeId;
}
