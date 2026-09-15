using FluentAssertions;
using JobMaster.NCrontab;

namespace JobMaster.UnitTests.RecurrenceExpressions;

public class NCrontabExprCompilerTests
{
    [Fact]
    public void TryCompile_ValidFiveFieldExpression_ReturnsCompiledExpr()
    {
        var compiler = new NCrontabExprCompiler();

        var compiled = compiler.TryCompile("*/6 * * * *");

        compiled.Should().NotBeNull();
        compiled!.Expression.Should().Be("*/6 * * * *");
        compiled.ExpressionTypeId.Should().Be(NCrontabExprCompiler.TypeId);
    }

    [Fact]
    public void TryCompile_ValidSixFieldExpression_DetectsSecondsPrecision()
    {
        var compiler = new NCrontabExprCompiler();
        var compiled = compiler.TryCompile("*/10 * * * * *");
        var baseDateTime = new DateTime(2024, 6, 15, 8, 30, 3, DateTimeKind.Unspecified);

        var next = compiled!.GetNextOccurrence(baseDateTime, "UTC");

        next.Should().Be(new DateTime(2024, 6, 15, 8, 30, 10, DateTimeKind.Unspecified));
    }

    [Fact]
    public void TryCompile_InvalidExpression_ReturnsNull()
    {
        var compiler = new NCrontabExprCompiler();

        var compiled = compiler.TryCompile("not a cron expression");

        compiled.Should().BeNull();
    }

    [Fact]
    public void Compile_InvalidExpression_Throws()
    {
        var compiler = new NCrontabExprCompiler();

        var act = () => compiler.Compile("not a cron expression");

        act.Should().Throw<Exception>();
    }

    [Fact]
    public void GetNextOccurrence_KnownPattern_ReturnsExpectedUnspecifiedKindDateTime()
    {
        var compiler = new NCrontabExprCompiler();
        var compiled = compiler.Compile("0 0 * * *");
        var baseDateTime = new DateTime(2024, 6, 15, 8, 30, 0, DateTimeKind.Unspecified);

        var next = compiled.GetNextOccurrence(baseDateTime, "America/New_York");

        next.Should().Be(new DateTime(2024, 6, 16, 0, 0, 0, DateTimeKind.Unspecified));
        next!.Value.Kind.Should().Be(DateTimeKind.Unspecified);
    }

    [Fact]
    public void GetNextOccurrence_IgnoresIanaTimeZoneId_TreatsInputAsWallClock()
    {
        var compiler = new NCrontabExprCompiler();
        var compiled = compiler.Compile("0 0 * * *");
        var baseDateTime = new DateTime(2024, 6, 15, 8, 30, 0, DateTimeKind.Unspecified);

        var nextUtc = compiled.GetNextOccurrence(baseDateTime, "UTC");
        var nextNy = compiled.GetNextOccurrence(baseDateTime, "America/New_York");

        nextUtc.Should().Be(nextNy);
    }

    [Fact]
    public void HasEnded_ForNormalExpression_ReturnsFalse()
    {
        var compiler = new NCrontabExprCompiler();
        var compiled = compiler.Compile("0 0 * * *");
        var baseDateTime = new DateTime(2024, 6, 15, 8, 30, 0, DateTimeKind.Unspecified);

        compiled.HasEnded(baseDateTime, "UTC").Should().BeFalse();
    }
}
