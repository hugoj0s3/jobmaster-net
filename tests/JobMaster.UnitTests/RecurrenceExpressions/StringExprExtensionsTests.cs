using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Cronos;
using JobMaster.NCrontab;
using JobMaster.RecurrenceExpressions.NaturalCron;
using Moq;

namespace JobMaster.UnitTests.RecurrenceExpressions;

// Every provider namespace is imported, so the prefixed string overloads are also checked to resolve unambiguously.
public class StringExprExtensionsTests
{
    // The factory is process-wide: don't replace a compiler another test class may have registered.
    static StringExprExtensionsTests()
    {
        RecurrenceCompilerFactory.RegisterCompiler(new CronosExprCompiler(), replaceIfExists: false);
        RecurrenceCompilerFactory.RegisterCompiler(new NCrontabExprCompiler(), replaceIfExists: false);
    }

    [Fact]
    public void PrefixedAdd_WithBothCronProvidersReferenced_UsesEachProvidersCompiler()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "cluster", workerLane: null);

        var definitions = new RecurringScheduleDefinitionCollection(profile, "cluster")
            .CronosAdd<PlainHandler>("*/6 * * * *", defId: "cronos")
            .NCrontabAdd<PlainHandler>("*/6 * * * *", defId: "ncrontab")
            .NaturalCronAdd<PlainHandler>("every 6 minutes", defId: "naturalcron")
            .ToReadOnly();

        definitions.Select(d => d.CompiledExpr.ExpressionTypeId).Should().Equal(
            CronosExprCompiler.TypeId, NCrontabExprCompiler.TypeId, NaturalCronExprCompiler.TypeId);
    }

    [Fact]
    public async Task PrefixedRecurringAsync_ForwardsCompiledExprOfThatProvider()
    {
        var scheduler = new Mock<IJobMasterScheduler>();

        await scheduler.Object.CronosRecurringAsync<PlainHandler>("*/6 * * * *");
        await scheduler.Object.NCrontabRecurringAsync<PlainHandler>("*/6 * * * *");
        await scheduler.Object.NaturalCronRecurringAsync<PlainHandler>("every 6 minutes");

        ForwardedTypeIds(scheduler, nameof(IJobMasterScheduler.RecurringAsync)).Should().Equal(
            CronosExprCompiler.TypeId, NCrontabExprCompiler.TypeId, NaturalCronExprCompiler.TypeId);
    }

    [Fact]
    public void PrefixedRecurring_ForwardsCompiledExprOfThatProvider()
    {
        var scheduler = new Mock<IJobMasterScheduler>();

        scheduler.Object.CronosRecurring<PlainHandler>("*/6 * * * *");
        scheduler.Object.NCrontabRecurring<PlainHandler>("*/6 * * * *");
        scheduler.Object.NaturalCronRecurring<PlainHandler>("every 6 minutes");

        ForwardedTypeIds(scheduler, nameof(IJobMasterScheduler.Recurring)).Should().Equal(
            CronosExprCompiler.TypeId, NCrontabExprCompiler.TypeId, NaturalCronExprCompiler.TypeId);
    }

    [Fact]
    public void NaturalCronRecurring_InvalidExpression_Throws()
    {
        var scheduler = new Mock<IJobMasterScheduler>();

        var act = () => scheduler.Object.NaturalCronRecurring<PlainHandler>("not a natural cron expression");

        act.Should().Throw<ArgumentException>();
        scheduler.Invocations.Should().BeEmpty();
    }

    [Fact]
    public void ObsoleteAdd_StillForwardsToPrefixedMethod()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "cluster", workerLane: null);
        var collection = new RecurringScheduleDefinitionCollection(profile, "cluster");

#pragma warning disable CS0618
        CronosExprExtensions.Add<PlainHandler>(collection, "*/6 * * * *", defId: "cronos");
        NCrontabExprExtensions.Add<PlainHandler>(collection, "*/6 * * * *", defId: "ncrontab");
#pragma warning restore CS0618

        collection.ToReadOnly().Select(d => d.CompiledExpr.ExpressionTypeId).Should().Equal(
            CronosExprCompiler.TypeId, NCrontabExprCompiler.TypeId);
    }

    private static IEnumerable<string> ForwardedTypeIds(Mock<IJobMasterScheduler> scheduler, string methodName)
        => scheduler.Invocations
            .Where(i => i.Method.Name == methodName)
            .Select(i => ((IRecurrenceCompiledExpr)i.Arguments[0]).ExpressionTypeId);

    private sealed class PlainHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
