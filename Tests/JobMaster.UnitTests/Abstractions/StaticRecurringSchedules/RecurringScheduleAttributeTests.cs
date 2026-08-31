using FluentAssertions;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.NaturalCron;
using JobMaster.RecurrenceExpressions.TimeSpanInterval;

namespace JobMaster.UnitTests.Abstractions.StaticRecurringSchedules;

public class RecurringScheduleAttributeTests
{
    [Fact]
    public void NaturalCronScheduleAttribute_ExposesExpressionAndTypeId()
    {
        var attr = new NaturalCronScheduleAttribute("every 6 minutes");

        attr.Expression.Should().Be("every 6 minutes");
        attr.ExpressionTypeId.Should().Be(NaturalCronExprCompiler.TypeId);
    }

    [Fact]
    public void TimeSpanIntervalScheduleAttribute_ExposesExpressionAndTypeId()
    {
        var attr = new TimeSpanIntervalScheduleAttribute("00:06:00");

        attr.Expression.Should().Be("00:06:00");
        attr.ExpressionTypeId.Should().Be(TimeSpanIntervalExprCompiler.TypeId);
    }
}
