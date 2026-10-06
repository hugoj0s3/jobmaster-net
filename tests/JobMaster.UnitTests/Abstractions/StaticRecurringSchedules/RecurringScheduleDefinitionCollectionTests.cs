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

    // Cluster id precedence: profile ClusterId → handler (JobDefinitionConfig / [JobMasterClusterId]) → default.

    [Fact]
    public void Add_WhenProfileHasClusterId_ProfileWinsOverHandlerAttribute()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "profile-cluster", workerLane: null);

        var definition = new RecurringScheduleDefinitionCollection(profile, "default-cluster")
            .Add(typeof(ClusterIdAttrHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        definition.ClusterId.Should().Be("profile-cluster");
        definition.Id.Should().StartWith("profile-cluster:");
    }

    [Fact]
    public void Add_WhenProfileHasNoClusterId_UsesHandlerAttributeCluster()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", clusterId: null, workerLane: null);

        var definition = new RecurringScheduleDefinitionCollection(profile, "default-cluster")
            .Add(typeof(ClusterIdAttrHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        definition.ClusterId.Should().Be("attr-cluster");
        definition.Id.Should().StartWith("attr-cluster:");
    }

    [Fact]
    public void Add_WhenProfileHasNoClusterId_UsesHandlerDefinitionConfigCluster()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", clusterId: null, workerLane: null);

        var definition = new RecurringScheduleDefinitionCollection(profile, "default-cluster")
            .Add(typeof(AdvancedHandlerWithCluster), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        definition.ClusterId.Should().Be("config-cluster");
    }

    [Fact]
    public void Add_WhenNeitherProfileNorHandlerHasClusterId_UsesDefaultCluster()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", clusterId: null, workerLane: null);

        var definition = new RecurringScheduleDefinitionCollection(profile, "default-cluster")
            .Add(typeof(PlainHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly()
            .Single();

        definition.ClusterId.Should().Be("default-cluster");
    }

    [Fact]
    public void Add_WhenProfileHasNoClusterId_SpansClustersPerHandler()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", clusterId: null, workerLane: null);

        var definitions = new RecurringScheduleDefinitionCollection(profile, "default-cluster")
            .Add(typeof(PlainHandler), "TimeSpanInterval", "00:06:00")
            .Add(typeof(ClusterIdAttrHandler), "TimeSpanInterval", "00:06:00")
            .ToReadOnly();

        definitions.Select(x => x.ClusterId).Should().BeEquivalentTo("default-cluster", "attr-cluster");
    }

    [Fact]
    public void ProfileInfo_WhenClusterIdBlank_TreatsAsNotSet()
    {
        var profile = new StaticRecurringSchedulesProfileInfo("profile", "  ", workerLane: null);

        profile.ClusterId.Should().BeNull();
        profile.IsValid.Should().BeTrue();
    }

    private sealed class PlainHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    [JobMasterClusterId("attr-cluster")]
    private sealed class ClusterIdAttrHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class FakeDefinitionWithClusterAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("advanced-cluster-defid", clusterId: "config-cluster");
    }

    [FakeDefinitionWithClusterAttribute]
    private sealed class AdvancedHandlerWithCluster : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }

    private sealed class FakeDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
    {
        public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("advanced-defid");
    }

    [FakeDefinitionAttribute]
    private sealed class AdvancedHandler : IJobMasterHandler
    {
        public Task HandleAsync(JobContext job) => Task.CompletedTask;
    }
}
