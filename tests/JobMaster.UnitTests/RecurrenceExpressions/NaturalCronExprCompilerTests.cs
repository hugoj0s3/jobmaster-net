
using FluentAssertions;
using JobMaster.RecurrenceExpressions.NaturalCron;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.RecurrenceExpressions;

public class NaturalCronExprCompilerTests
{
    [Fact]
    public void TryCompile_ValidExpression_ReturnsCompiledExpr()
    {
        var compiler = new NaturalCronExprCompiler();

        var compiled = compiler.TryCompile("every 5 minutes");

        compiled.Should().NotBeNull();
        compiled!.Expression.Should().Be("every 5 minutes");
        compiled.ExpressionTypeId.Should().Be(NaturalCronExprCompiler.TypeId);
    }

    [Fact]
    public void TryCompile_InvalidExpression_ReturnsNull()
    {
        var compiler = new NaturalCronExprCompiler();

        compiler.TryCompile("not a natural cron expression").Should().BeNull();
    }

    // NaturalCron PR #8 (1.0.2): occurrences without a tz rule came back as Unspecified. JobMaster reads the
    // wall-clock result and converts it to UTC itself, so the planner's result must be the right UTC instant.
    [Fact]
    public void GetNextOccurrence_NoTzRule_ConvertsToExpectedUtcInstant()
    {
        var compiled = new NaturalCronExprCompiler().Compile("daily at 09:00");
        var baseUtc = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc); // 08:00 in New York (EDT)
        var baseInTz = TimeZoneUtils.ConvertUtcToDateTimeTz(baseUtc, "America/New_York");

        var nextInTz = compiled.GetNextOccurrence(baseInTz, "America/New_York");
        var nextUtc = TimeZoneUtils.ConvertDateTimeTzToUtc(nextInTz!.Value, "America/New_York");

        nextInTz.Value.Should().Be(new DateTime(2024, 6, 15, 9, 0, 0));
        nextUtc.Should().Be(new DateTime(2024, 6, 15, 13, 0, 0, DateTimeKind.Utc));
        nextUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    // Unlike Cronos/NCrontab, a tz rule in the expression wins over the schedule's time zone: the occurrence is computed
    // in the expression's zone and returned as wall-clock in the time zone that was passed in.
    [Fact]
    public void GetNextOccurrence_ExpressionTzDiffersFromScheduleTz_UsesExpressionTz()
    {
        var compiled = new NaturalCronExprCompiler().Compile("daily at 09:00 tz Asia/Tokyo");
        var baseUtc = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc); // 08:00 New York, 21:00 Tokyo
        var baseInTz = TimeZoneUtils.ConvertUtcToDateTimeTz(baseUtc, "America/New_York");

        var nextInTz = compiled.GetNextOccurrence(baseInTz, "America/New_York");
        var nextUtc = TimeZoneUtils.ConvertDateTimeTzToUtc(nextInTz!.Value, "America/New_York");

        // 09:00 Tokyo on the 16th = 00:00 UTC on the 16th = 20:00 New York on the 15th.
        nextInTz.Value.Should().Be(new DateTime(2024, 6, 15, 20, 0, 0));
        nextUtc.Should().Be(new DateTime(2024, 6, 16, 0, 0, 0, DateTimeKind.Utc));
    }

    // NaturalCron 1.1.0: "every <weekday> at <time>" returned the day after the weekday (1.0.1 gave Tuesday here).
    [Fact]
    public void GetNextOccurrence_EveryWeekdayAtTime_ReturnsThatWeekday()
    {
        var compiled = new NaturalCronExprCompiler().Compile("every monday at 08:00");
        var sunday = new DateTime(2024, 6, 16, 9, 0, 0, DateTimeKind.Unspecified);

        var next = compiled.GetNextOccurrence(sunday, "UTC");

        next.Should().Be(new DateTime(2024, 6, 17, 8, 0, 0));
        next!.Value.DayOfWeek.Should().Be(DayOfWeek.Monday);
    }
}
