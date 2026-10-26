using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Cronos;
using JobMaster.NCrontab;
using JobMaster.RecurrenceExpressions.NaturalCron;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace JobMaster.UnitTests.RecurrenceExpressions;

// Both JobMaster.Cronos and JobMaster.NCrontab are imported on purpose: the prefixed string overloads must resolve
// without ambiguity when both packages are referenced (the unprefixed ones don't, which is why they're obsolete).
public class StringExprExtensionsTests
{
    static StringExprExtensionsTests()
    {
        new ServiceCollection().AddJobMasterCronos().AddJobMasterNCrontab();
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

        act.Should().Throw<Exception>();
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
