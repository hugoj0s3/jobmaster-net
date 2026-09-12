using JobMaster.RecurrenceExpressions.TimeSpanInterval;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Declares a static recurring schedule using a fixed <see cref="TimeSpan"/> interval expression
/// (e.g. <c>"00:06:00"</c>) directly on the job handler class. See <see cref="RecurringScheduleAttribute"/>.
/// </summary>
public sealed class TimeSpanIntervalScheduleAttribute : RecurringScheduleAttribute
{
    public TimeSpanIntervalScheduleAttribute(string expression) : base(expression)
    {
    }

    public override string ExpressionTypeId => TimeSpanIntervalExprCompiler.TypeId;
}
