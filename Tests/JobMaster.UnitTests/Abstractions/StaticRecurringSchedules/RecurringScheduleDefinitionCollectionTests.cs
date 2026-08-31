using FluentAssertions;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.UnitTests.Abstractions.StaticRecurringSchedules;

public class RecurringScheduleDefinitionCollectionTests
{
    [Fact]
    public void Add_TypeBased_ProducesSameDefinitionAs_GenericOverload()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "cluster", workerLane: null);

        var viaGeneric = new RecurringScheduleDefinitionCollection(profile, "cluster")
            .Add<PlainHandler>("TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        var viaType = new RecurringScheduleDefinitionCollection(profile, "cluster")
            .Add(typeof(PlainHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        viaType.JobDefinitionId.Should().Be(viaGeneric.JobDefinitionId);
        viaType.Id.Should().Be(viaGeneric.Id);
        viaType.CompiledExpr.ExpressionTypeId.Should().Be(viaGeneric.CompiledExpr.ExpressionTypeId);
        viaType.CompiledExpr.Expression.Should().Be(viaGeneric.CompiledExpr.Expression);
    }

    [Fact]
    public void Add_WhenHandlerHasJobDefinitionConfigAttribute_UsesItsJobDefinitionId()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "cluster", workerLane: null);

        var definition = new RecurringScheduleDefinitionCollection(profile, "cluster")
            .Add(typeof(AdvancedHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        definition.JobDefinitionId.Should().Be("advanced-defid");
    }

    [Fact]
    public void Add_WhenHandlerTypeDoesNotImplementIJobMasterHandler_Throws()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "cluster", workerLane: null);
        var collection = new RecurringScheduleDefinitionCollection(profile, "cluster");

        var act = () => collection.Add(typeof(string), "TimeSpanInterval", "00:06:00");

        act.Should().Throw<ArgumentException>();
    }

    private sealed class PlainHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class FakeDefinitionAttribute : JobDefinitionConfigAttribute
    {
        public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig("advanced-defid");
    }

    [FakeDefinitionAttribute]
    private sealed class AdvancedHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
