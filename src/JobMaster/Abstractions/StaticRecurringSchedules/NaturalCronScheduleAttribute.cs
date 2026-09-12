using JobMaster.RecurrenceExpressions.NaturalCron;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Declares a static recurring schedule using a NaturalCron expression (e.g. <c>"every 6 minutes"</c>)
/// directly on the job handler class. See <see cref="RecurringScheduleAttribute"/>.
/// </summary>
public sealed class NaturalCronScheduleAttribute : RecurringScheduleAttribute
{
    public NaturalCronScheduleAttribute(string expression) : base(expression)
    {
    }

    public override string ExpressionTypeId => NaturalCronExprCompiler.TypeId;
}
