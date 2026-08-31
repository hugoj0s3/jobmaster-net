using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Jobs;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class RecurringScheduleTests
{
    [Fact]
    public void New_Generic_WhenJobDefinitionConfigAttributePresent_UsesConfigId()
    {
        var rec = RecurringSchedule.New<HandlerWithConfigAttribute>(
            "cluster",
            values: null,
            expression: new NeverRecursCompiledExpr(),
            priority: null,
            timeout: null,
            maxNumberOfRetries: null,
            metadata: null,
            recurringScheduleType: RecurringScheduleType.Dynamic,
            staticDefinitionId: null,
            startAfter: null,
            endBefore: null,
            workerLane: null);

        rec.JobDefinitionId.Should().Be("recurring-config-defid");
    }

    [Fact]
    public void New_WithConfig_UsesConfigValues()
    {
        var config = new JobDefinitionConfig(
            "orders.recur",
            priority: JobMasterPriority.High,
            timeout: TimeSpan.FromSeconds(30),
            maxNumberOfRetries: 4,
            workerLane: "orders");

        var rec = RecurringSchedule.New(
            "cluster",
            config,
            values: null,
            expression: new NeverRecursCompiledExpr(),
            recurringScheduleType: RecurringScheduleType.Dynamic,
            staticDefinitionId: null,
            startAfter: null,
            endBefore: null);

        rec.JobDefinitionId.Should().Be("orders.recur");
        rec.Priority.Should().Be(JobMasterPriority.High);
        rec.Timeout.Should().Be(TimeSpan.FromSeconds(30));
        rec.MaxNumberOfRetries.Should().Be(4);
        rec.WorkerLane.Should().Be("orders");
    }

    private class FakeDefinitionAttribute : JobDefinitionConfigAttribute
    {
        public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig("recurring-config-defid");
    }

    [FakeDefinitionAttribute]
    private class HandlerWithConfigAttribute : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
